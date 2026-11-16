//! The local server as the web app uses it: tables listed and opened over the WebSocket,
//! windows pushed after cycles, and the app's files.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, AsArray, Float64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use futures_util::{SinkExt, StreamExt};
use kermodes_core::Session;
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Socket = WebSocketStream<MaybeTlsStream<TcpStream>>;

enum Received {
    Text(Value),
    Window {
        header: Value,
        rows: RecordBatch,
        keys: Vec<u64>,
    },
}

async fn next(socket: &mut Socket) -> Received {
    let message = tokio::time::timeout(Duration::from_secs(30), socket.next())
        .await
        .expect("the server answers")
        .expect("the socket stays open")
        .unwrap();
    match message {
        Message::Text(text) => Received::Text(serde_json::from_str(&text).unwrap()),
        Message::Binary(bytes) => {
            let length = |at: usize| u32::from_le_bytes(bytes[at..at + 4].try_into().unwrap());
            let head = length(0) as usize;
            let header: Value = serde_json::from_slice(&bytes[4..4 + head]).unwrap();
            let rows_at = 8 + head;
            let rows_end = rows_at + length(4 + head) as usize;
            let mut reader = StreamReader::try_new(&bytes[rows_at..rows_end], None).unwrap();
            let rows = reader.next().unwrap().unwrap();
            let (keys, rest) = bytes[rows_end..].as_chunks::<8>();
            assert!(rest.is_empty());
            let keys = keys.iter().map(|k| u64::from_le_bytes(*k)).collect();
            Received::Window { header, rows, keys }
        }
        other => panic!("unexpected {other:?}"),
    }
}

async fn text(socket: &mut Socket) -> Value {
    match next(socket).await {
        Received::Text(value) => value,
        Received::Window { header, .. } => panic!("expected text, got a window {header}"),
    }
}

/// The state message and the window that follows it.
async fn window(socket: &mut Socket) -> (Value, Value, RecordBatch, Vec<u64>) {
    let state = text(socket).await;
    assert_eq!(state["type"], "state", "{state}");
    match next(socket).await {
        Received::Window { header, rows, keys } => (state, header, rows, keys),
        Received::Text(value) => panic!("expected a window, got {value}"),
    }
}

async fn send(socket: &mut Socket, message: Value) {
    socket
        .send(Message::Text(message.to_string().into()))
        .await
        .unwrap();
}

fn syms(rows: &RecordBatch) -> Vec<&str> {
    rows.column(0).as_string::<i32>().iter().flatten().collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn the_app_lists_opens_filters_and_follows_published_tables() {
    let session = Session::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
    ]));
    let book = session.keyed_input_table(&schema, &["sym"]).unwrap();
    let batch = |syms: Vec<&str>, prices: Vec<f64>| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(syms)) as ArrayRef,
                Arc::new(Float64Array::from(prices)),
            ],
        )
        .unwrap()
    };
    book.upsert(&[batch(vec!["aa", "AB", "b"], vec![3.0, 1.0, 4.0])])
        .unwrap();
    session.step();
    let mut server = kermodes_gateway::serve(
        session.clone(),
        "127.0.0.1:0".parse().unwrap(),
        None,
        None,
        None,
    )
    .unwrap();
    server.publish("book", &book);
    // A page's view (the Deployments tab): named for the app, not listed among tables.
    server.publish_view("page", &book);

    let url = format!("ws://{}/ws", server.addr());
    let (mut socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let tables = text(&mut socket).await;
    assert_eq!(tables["type"], "tables");
    assert_eq!(tables["tables"].as_array().unwrap().len(), 1);
    assert_eq!(tables["tables"][0]["name"], "book");
    assert_eq!(tables["tables"][0]["refreshing"], true);
    assert_eq!(tables["tables"][0]["columns"][1]["type"], "float64");
    assert_eq!(tables["views"], json!(["page"]));

    send(
        &mut socket,
        json!({"type": "open", "view": 1, "table": "book"}),
    )
    .await;
    let (state, header, rows, keys) = window(&mut socket).await;
    assert_eq!(
        (state["view"].clone(), state["row_count"].clone()),
        (json!(1), json!(3))
    );
    assert_eq!(
        (header["type"].clone(), header["total"].clone()),
        (json!("rows"), json!(3))
    );
    assert_eq!(syms(&rows), ["aa", "AB", "b"]);
    let key_of_ab = keys[1];

    send(
        &mut socket,
        json!({"type": "filter", "view": 1, "filters": {"sym": "a", "price": "> x"}}),
    )
    .await;
    let (state, _, rows, _) = window(&mut socket).await;
    assert_eq!(syms(&rows), ["aa", "AB"]);
    assert_eq!(state["filter_errors"]["price"], "`x` is not a number");

    // A cycle that changes the table pushes the window without being asked.
    book.upsert(&[batch(vec!["AB"], vec![9.0])]).unwrap();
    session.step();
    let (_, header, rows, keys) = window(&mut socket).await;
    assert_eq!(header["cycle"], 2);
    assert_eq!(
        rows.column(1)
            .as_primitive::<arrow::datatypes::Float64Type>()
            .value(1),
        9.0
    );
    assert_eq!(keys[1], key_of_ab);

    send(
        &mut socket,
        json!({"type": "fetch", "view": 1, "id": 4, "start": 1, "count": 1}),
    )
    .await;
    match next(&mut socket).await {
        Received::Window { header, rows, .. } => {
            assert_eq!(
                (header["type"].clone(), header["id"].clone()),
                (json!("fetched"), json!(4))
            );
            assert_eq!(syms(&rows), ["AB"]);
        }
        Received::Text(value) => panic!("expected the fetched rows, got {value}"),
    }

    send(
        &mut socket,
        json!({"type": "open", "view": 2, "table": "nope"}),
    )
    .await;
    let missing = text(&mut socket).await;
    assert_eq!(
        (missing["type"].clone(), missing["view"].clone()),
        (json!("error"), json!(2))
    );

    // A page's view opens by name like a table.
    send(
        &mut socket,
        json!({"type": "open", "view": 3, "table": "page"}),
    )
    .await;
    let (state, _, rows, _) = window(&mut socket).await;
    assert_eq!((state["view"].clone(), syms(&rows).len()), (json!(3), 3));
    send(&mut socket, json!({"type": "close", "view": 3})).await;

    assert!(server.unpublish("book"));
    let tables = text(&mut socket).await;
    assert_eq!(tables["tables"], json!([]));
    server.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn a_chart_follows_its_table_through_a_series() {
    let session = Session::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("t", DataType::Float64, true),
        Field::new("v", DataType::Float64, true),
    ]));
    let line = session.keyed_input_table(&schema, &["t"]).unwrap();
    let batch = |ts: Vec<f64>, vs: Vec<f64>| {
        RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Float64Array::from(ts)) as ArrayRef,
                Arc::new(Float64Array::from(vs)),
            ],
        )
        .unwrap()
    };
    // Written out of x order: the points come back in it.
    line.upsert(&[batch(vec![3.0, 1.0, 2.0], vec![30.0, 10.0, 20.0])])
        .unwrap();
    session.step();
    let mut server = kermodes_gateway::serve(
        session.clone(),
        "127.0.0.1:0".parse().unwrap(),
        None,
        None,
        None,
    )
    .unwrap();
    server.publish("line", &line);
    let url = format!("ws://{}/ws", server.addr());
    let (mut socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    text(&mut socket).await;

    let column = |rows: &RecordBatch, at: usize| -> Vec<f64> {
        rows.column(at)
            .as_primitive::<arrow::datatypes::Float64Type>()
            .values()
            .to_vec()
    };
    send(
        &mut socket,
        json!({"type": "series", "view": 1, "table": "line", "x": "t", "y": ["v"]}),
    )
    .await;
    let (state, header, rows, keys) = window(&mut socket).await;
    assert_eq!(state["row_count"], 3);
    assert_eq!(header["type"], "series");
    assert_eq!(column(&rows, 0), [1.0, 2.0, 3.0]);
    assert_eq!(column(&rows, 1), [10.0, 20.0, 30.0]);
    assert!(keys.is_empty());

    // A change to the table pushes the points again, unasked.
    line.upsert(&[batch(vec![2.0], vec![25.0])]).unwrap();
    line.delete(&[RecordBatch::try_from_iter([(
        "t",
        Arc::new(Float64Array::from(vec![1.0])) as ArrayRef,
    )])
    .unwrap()])
        .unwrap();
    session.step();
    let (_, _, rows, _) = window(&mut socket).await;
    assert_eq!(column(&rows, 0), [2.0, 3.0]);
    assert_eq!(column(&rows, 1), [25.0, 30.0]);

    // Zoomed to a range: answered at once with the rows in it, and back out with null
    send(
        &mut socket,
        json!({"type": "range", "view": 1, "x_range": [2.5, 3.0]}),
    )
    .await;
    let (_, _, rows, _) = window(&mut socket).await;
    assert_eq!(column(&rows, 0), [3.0]);
    send(
        &mut socket,
        json!({"type": "range", "view": 1, "x_range": null}),
    )
    .await;
    let (_, _, rows, _) = window(&mut socket).await;
    assert_eq!(column(&rows, 0), [2.0, 3.0]);
    send(
        &mut socket,
        json!({"type": "range", "view": 1, "x_range": [3.0, 2.0]}),
    )
    .await;
    assert_eq!(text(&mut socket).await["type"], "error");

    // A chart is not scrolled or sorted; and it names what it cannot draw.
    send(
        &mut socket,
        json!({"type": "viewport", "view": 1, "top": 0, "bottom": 10}),
    )
    .await;
    assert_eq!(text(&mut socket).await["type"], "error");
    for (view, asked) in [
        (
            2,
            json!({"type": "series", "view": 2, "table": "line", "x": "t", "y": "nope"}),
        ),
        (
            3,
            json!({"type": "series", "view": 3, "table": "line", "x": "t"}),
        ),
        (
            4,
            json!({"type": "series", "view": 4, "table": "gone", "x": "t", "y": "v"}),
        ),
    ] {
        send(&mut socket, asked).await;
        let refused = text(&mut socket).await;
        assert_eq!(
            (refused["type"].clone(), refused["view"].clone()),
            (json!("error"), json!(view)),
            "{refused}"
        );
    }
    server.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_app_is_served_from_its_directory_only() {
    let app = std::env::temp_dir().join(format!("kermodes-gateway-app-{}", std::process::id()));
    std::fs::create_dir_all(app.join("assets")).unwrap();
    std::fs::write(app.join("index.html"), "<!doctype html>kermodes").unwrap();
    std::fs::write(app.join("assets/app.js"), "export {}").unwrap();
    let server = kermodes_gateway::serve(
        Session::new(),
        "127.0.0.1:0".parse().unwrap(),
        Some(app.clone()),
        None,
        None,
    )
    .unwrap();
    let get = |path: &str| {
        let addr = server.addr();
        let path = path.to_owned();
        async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut stream = TcpStream::connect(addr).await.unwrap();
            let request = format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).await.unwrap();
            response
        }
    };
    let index = get("/").await;
    assert!(index.starts_with("HTTP/1.1 200"), "{index}");
    assert!(
        index.contains("text/html") && index.ends_with("kermodes"),
        "{index}"
    );
    let script = get("/assets/app.js").await;
    assert!(script.contains("text/javascript"), "{script}");
    assert!(get("/../Cargo.toml").await.starts_with("HTTP/1.1 404"));
    assert!(
        get("/assets/%2e%2e/%2e%2e/index.html")
            .await
            .starts_with("HTTP/1.1 404")
    );
    std::fs::remove_dir_all(app).unwrap();
}

/// An app's own process (`serve_at`) serves its built frontend and its tables under the app's
/// path, where the cluster's route sends its requests unchanged; nothing outside it.
#[tokio::test(flavor = "multi_thread")]
async fn an_apps_process_serves_its_page_and_tables_under_its_path() {
    let app = std::env::temp_dir().join(format!("kermodes-gateway-own-{}", std::process::id()));
    std::fs::create_dir_all(app.join("assets")).unwrap();
    std::fs::write(app.join("index.html"), "<!doctype html>market").unwrap();
    std::fs::write(app.join("assets/app.js"), "export {}").unwrap();
    let session = Session::new();
    let schema = Arc::new(Schema::new(vec![Field::new("sym", DataType::Utf8, true)]));
    let spreads = session.keyed_input_table(&schema, &["sym"]).unwrap();
    session.step();
    let base = "/apps/ticker/market/";
    let server = kermodes_gateway::serve_at(
        session,
        "127.0.0.1:0".parse().unwrap(),
        base,
        Some(app.clone()),
        None,
    )
    .unwrap();
    server.publish("spreads", &spreads);
    let get = |path: &str| {
        let addr = server.addr();
        let path = path.to_owned();
        async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut stream = TcpStream::connect(addr).await.unwrap();
            let request = format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).await.unwrap();
            response
        }
    };
    let page = get(base).await;
    assert!(
        page.starts_with("HTTP/1.1 200") && page.ends_with("market"),
        "{page}"
    );
    let script = get("/apps/ticker/market/assets/app.js").await;
    assert!(script.contains("text/javascript"), "{script}");
    // A page of the app's own router is answered by its index; a missing file is not.
    assert!(
        get("/apps/ticker/market/quotes/AAPL")
            .await
            .ends_with("market")
    );
    assert!(
        get("/apps/ticker/market/assets/gone.js")
            .await
            .starts_with("HTTP/1.1 404")
    );
    let bare = get("/apps/ticker/market").await;
    assert!(
        bare.starts_with("HTTP/1.1 308") && bare.contains("location: /apps/ticker/market/"),
        "{bare}"
    );
    for outside in [
        "/",
        "/ws",
        "/assets/app.js",
        "/api/components",
        "/apps/ticker/other/",
    ] {
        assert!(get(outside).await.starts_with("HTTP/1.1 404"), "{outside}");
    }

    let url = format!("ws://{}{base}ws", server.addr());
    let (mut socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let tables = text(&mut socket).await;
    assert_eq!(tables["type"], "tables");
    assert_eq!(tables["tables"][0]["name"], "spreads");
    std::fs::remove_dir_all(app).unwrap();
}

/// An app `kermodes dev` runs is reached through the gateway at the app's address: its tables'
/// WebSocket goes to its own process; pages with their path and query, and the hot-reload
/// WebSocket with its subprotocol, go to its dev server.
#[tokio::test(flavor = "multi_thread")]
async fn an_apps_dev_server_is_reached_through_the_gateway() {
    use axum::extract::ws::{Message as Ws, WebSocketUpgrade};
    use tokio_tungstenite::tungstenite::client::IntoClientRequest;
    // Stands in for Vite: says what it was asked, and echoes on a socket only with its protocol
    let dev = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let dev_url = format!("http://{}", dev.local_addr().unwrap());
    let hmr = |ws: WebSocketUpgrade| async move {
        ws.protocols(["vite-hmr"])
            .on_upgrade(|mut socket| async move {
                while let Some(Ok(Ws::Text(text))) = socket.recv().await {
                    let echo = format!("echo {}", text.as_str());
                    if socket.send(Ws::text(echo)).await.is_err() {
                        break;
                    }
                }
            })
    };
    let router = axum::Router::new()
        .route("/apps/ticker/market/hmr", axum::routing::get(hmr))
        .fallback(|uri: axum::http::Uri| async move { format!("dev server: {uri}") });
    tokio::spawn(async move { axum::serve(dev, router).await.unwrap() });
    // Stands in for the app's own process: answers on its tables' WebSocket.
    let process = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let process_url = format!("http://{}", process.local_addr().unwrap());
    let tables = |ws: WebSocketUpgrade| async move {
        ws.on_upgrade(|mut socket| async move {
            let _ = socket.send(Ws::text("the app's tables")).await;
        })
    };
    let router = axum::Router::new().route("/apps/ticker/market/ws", axum::routing::get(tables));
    tokio::spawn(async move { axum::serve(process, router).await.unwrap() });

    let server = kermodes_gateway::serve(
        Session::new(),
        "127.0.0.1:0".parse().unwrap(),
        None,
        None,
        None,
    )
    .unwrap();
    let get = |path: &str| {
        let addr = server.addr();
        let path = path.to_owned();
        async move {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut stream = TcpStream::connect(addr).await.unwrap();
            let request = format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).await.unwrap();
            response
        }
    };
    // Not an app until a dev server runs it
    assert!(
        get("/apps/ticker/market/")
            .await
            .starts_with("HTTP/1.1 404")
    );

    server.serve_app("ticker", "market", &dev_url, &process_url);
    let listed = get("/api/apps").await;
    assert!(
        listed.ends_with(r#"[{"name":"market","project":"ticker"}]"#),
        "{listed}"
    );
    let page = get("/apps/ticker/market/src/main.tsx?t=1").await;
    assert!(
        page.starts_with("HTTP/1.1 200")
            && page.ends_with("dev server: /apps/ticker/market/src/main.tsx?t=1"),
        "{page}"
    );
    // Relative assets resolve under the page only with its trailing slash
    let bare = get("/apps/ticker/market").await;
    assert!(
        bare.starts_with("HTTP/1.1 308") && bare.contains("location: /apps/ticker/market/"),
        "{bare}"
    );
    for outside in [
        "/apps/ticker/other/",
        "/apps/%2e%2e/market/",
        "/apps/ticker/",
    ] {
        assert!(get(outside).await.starts_with("HTTP/1.1 404"), "{outside}");
    }

    let mut request = format!("ws://{}/apps/ticker/market/hmr", server.addr())
        .into_client_request()
        .unwrap();
    request
        .headers_mut()
        .insert("sec-websocket-protocol", "vite-hmr".parse().unwrap());
    let (mut socket, response) = tokio_tungstenite::connect_async(request).await.unwrap();
    assert_eq!(response.headers()["sec-websocket-protocol"], "vite-hmr");
    socket.send(Message::text("update")).await.unwrap();
    let echoed = tokio::time::timeout(Duration::from_secs(10), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(echoed, Message::text("echo update"));
    let url = format!("ws://{}/apps/ticker/market/ws", server.addr());
    let (mut socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let first = tokio::time::timeout(Duration::from_secs(10), socket.next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(first, Message::text("the app's tables"));

    assert!(server.stop_app("ticker", "market"));
    assert!(
        get("/apps/ticker/market/")
            .await
            .starts_with("HTTP/1.1 404")
    );
}

/// The controller's API is reachable only through the routes the app uses, with names checked
/// and parameters re-encoded: a component's logs keep `pod` and `tail`, and nothing else.
#[tokio::test(flavor = "multi_thread")]
async fn only_the_apps_controller_routes_reach_the_controller() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    // A controller that answers everything and records what it was asked.
    let controller = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}", controller.local_addr().unwrap());
    let asked = Arc::new(std::sync::Mutex::new(Vec::<String>::new()));
    let record = asked.clone();
    tokio::spawn(async move {
        loop {
            let (mut stream, _) = controller.accept().await.unwrap();
            let mut request = vec![0u8; 4096];
            let n = stream.read(&mut request).await.unwrap();
            let line = String::from_utf8_lossy(&request[..n]).into_owned();
            let path = line
                .split_whitespace()
                .nth(1)
                .unwrap_or_default()
                .to_owned();
            record.lock().unwrap().push(path);
            let answer = b"HTTP/1.1 200 OK\r\ncontent-length: 2\r\nconnection: close\r\n\r\nok";
            stream.write_all(answer).await.unwrap();
        }
    });
    let server = kermodes_gateway::serve(
        Session::new(),
        "127.0.0.1:0".parse().unwrap(),
        None,
        Some(url),
        None,
    )
    .unwrap();
    let status = |path: &str| {
        let addr = server.addr();
        let path = path.to_owned();
        async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            let request = format!("GET {path} HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n");
            stream.write_all(request.as_bytes()).await.unwrap();
            let mut response = String::new();
            stream.read_to_string(&mut response).await.unwrap();
            response
                .split_whitespace()
                .nth(1)
                .unwrap_or_default()
                .to_owned()
        }
    };
    assert_eq!(status("/api/components").await, "200");
    assert_eq!(
        status("/api/components/weir/logs?pod=kermodes-weir-0&tail=50&follow=1").await,
        "200"
    );
    assert_eq!(
        status("/api/components/weir/logs?pod=..%2F..%2Fsnapshots&tail=all").await,
        "200"
    );
    assert_eq!(status("/api/components/weir/events").await, "200");
    assert_eq!(status("/api/components/weir/restart").await, "404");
    assert_eq!(status("/api/components/..%2Fsnapshots/events").await, "400");
    assert_eq!(
        *asked.lock().unwrap(),
        [
            "/api/components",
            "/api/components/weir/logs?pod=kermodes-weir-0&tail=50",
            "/api/components/weir/logs",
            "/api/components/weir/events",
        ]
    );
}

/// Stored tables are listed without being opened, opened once however many views show one,
/// and let go when the last view closes; one that fails (its store restarted) ends its views
/// with an error the app retries, and the next view opens it afresh.
#[tokio::test(flavor = "multi_thread")]
async fn stored_tables_are_open_only_while_viewed() {
    let session = Session::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
    ]));
    // Each opening's feed, as the store would hold it.
    let opened = Arc::new(std::sync::Mutex::new(Vec::<kermodes_core::Feed>::new()));
    let stored = {
        let (session, opened) = (session.clone(), opened.clone());
        kermodes_gateway::Stored {
            list: Box::new(|| {
                let columns = vec![
                    ("sym".to_owned(), "utf8".to_owned()),
                    ("price".to_owned(), "float64".to_owned()),
                ];
                Ok(vec![("md.ticks".to_owned(), columns)])
            }),
            open: Box::new(move |name| {
                let (table, feed) = session
                    .fed_table(&schema, &format!("live {name}"))
                    .map_err(|e| e.to_string())?;
                feed.ready();
                opened.lock().unwrap().push(feed);
                Ok(table)
            }),
            // As a keyed table: opened live, which this test is about.
            positional: Box::new(|_| Ok(None)),
            rows: Box::new(|_, _, _| Err("keyed tables are not read by position".to_owned())),
            stats: Box::new(|| Ok(json!({"tables": [{"table": "md.ticks", "weir": []}]}))),
            idle: Duration::ZERO,
        }
    };
    let server = kermodes_gateway::serve(
        session.clone(),
        "127.0.0.1:0".parse().unwrap(),
        None,
        None,
        Some(stored),
    )
    .unwrap();
    let url = format!("ws://{}/ws", server.addr());
    let (mut socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    // Listed once the server has asked the store, with nothing opened.
    loop {
        let tables = text(&mut socket).await;
        if tables["tables"][0]["name"] == "md.ticks" {
            assert_eq!(tables["tables"][0]["columns"][1]["type"], "float64");
            break;
        }
    }
    let openings = || opened.lock().unwrap().len();
    assert_eq!(openings(), 0);

    for view in [1, 2] {
        send(
            &mut socket,
            json!({"type": "open", "view": view, "table": "md.ticks"}),
        )
        .await;
        let (state, ..) = window(&mut socket).await;
        assert_eq!(state["view"], view);
    }
    assert_eq!(openings(), 1, "two views of one table open it once");

    for view in [1, 2] {
        send(&mut socket, json!({"type": "close", "view": view})).await;
    }
    send(
        &mut socket,
        json!({"type": "open", "view": 3, "table": "md.ticks"}),
    )
    .await;
    window(&mut socket).await;
    assert_eq!(openings(), 2, "closing the last view let it go");

    // The store restarts: the table fails, and its view ends with an error to retry.
    let feed = opened.lock().unwrap()[1].clone();
    feed.fail("weir restarted".into());
    session.step();
    let ended = text(&mut socket).await;
    assert_eq!(
        (
            ended["type"].clone(),
            ended["view"].clone(),
            ended["retry"].clone()
        ),
        (json!("error"), json!(3), json!(true)),
        "{ended}"
    );
    assert!(
        ended["message"]
            .as_str()
            .unwrap()
            .contains("weir restarted")
    );
    send(
        &mut socket,
        json!({"type": "open", "view": 4, "table": "md.ticks"}),
    )
    .await;
    window(&mut socket).await;
    assert_eq!(openings(), 3, "a failed table is opened afresh");

    send(
        &mut socket,
        json!({"type": "open", "view": 5, "table": "md.nothing"}),
    )
    .await;
    let missing = text(&mut socket).await;
    assert_eq!(missing["retry"], Value::Null, "{missing}");

    // The Catalog tab's list comes from the store's stats, not from opening tables.
    let catalog = {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let mut stream = TcpStream::connect(server.addr()).await.unwrap();
        let request = "GET /api/catalog HTTP/1.1\r\nHost: x\r\nConnection: close\r\n\r\n";
        stream.write_all(request.as_bytes()).await.unwrap();
        let mut response = String::new();
        stream.read_to_string(&mut response).await.unwrap();
        response
    };
    assert!(catalog.starts_with("HTTP/1.1 200"), "{catalog}");
    assert!(
        catalog.ends_with(r#"{"tables":[{"table":"md.ticks","weir":[]}]}"#),
        "{catalog}"
    );
    assert_eq!(openings(), 4 - 1, "listing the catalog opened nothing");
}

/// A stored append table is shown by position: its window comes from the store and nothing is
/// opened; the view follows the table as it grows and reads wherever the grid scrolls, keyed
/// by the rows' positions. A sort opens the table live; clearing it goes back to the window and
/// lets the table go.
#[tokio::test(flavor = "multi_thread")]
async fn stored_append_tables_are_shown_by_position() {
    let session = Session::new();
    let schema = Arc::new(Schema::new(vec![
        Field::new("sym", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
    ]));
    // The store's day, by position: r0 at 0.0, r1 at 1.0, ...
    let day = Arc::new(std::sync::Mutex::new(250_usize));
    let rows = {
        let schema = schema.clone();
        move |from: usize, to: usize| {
            let syms: Vec<String> = (from..to).map(|i| format!("r{i}")).collect();
            let prices: Vec<f64> = (from..to).map(|i| i as f64).collect();
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(StringArray::from(syms)) as ArrayRef,
                    Arc::new(Float64Array::from(prices)),
                ],
            )
            .unwrap()
        }
    };
    let live = Arc::new(std::sync::Mutex::new(Vec::<kermodes_core::WeakTable>::new()));
    let stored = {
        let (session, live, day, rows, schema) = (
            session.clone(),
            live.clone(),
            day.clone(),
            rows.clone(),
            schema.clone(),
        );
        let read = (day.clone(), rows.clone());
        kermodes_gateway::Stored {
            list: Box::new(|| {
                let columns = vec![
                    ("sym".to_owned(), "utf8".to_owned()),
                    ("price".to_owned(), "float64".to_owned()),
                ];
                Ok(vec![("md.ticks".to_owned(), columns)])
            }),
            open: Box::new(move |name| {
                let (table, feed) = session
                    .fed_table(&schema, &format!("live {name}"))
                    .map_err(|e| e.to_string())?;
                feed.append(&[rows(0, *day.lock().unwrap())]).unwrap();
                feed.ready();
                live.lock().unwrap().push(table.downgrade());
                std::mem::forget(feed);
                Ok(table)
            }),
            positional: {
                let schema = Arc::new(Schema::new(vec![
                    Field::new("sym", DataType::Utf8, true),
                    Field::new("price", DataType::Float64, true),
                ]));
                Box::new(move |_| Ok(Some(schema.clone())))
            },
            rows: Box::new(move |_, start, count| {
                let total = *read.0.lock().unwrap();
                let from = (start as usize).min(total);
                let to = (start + count).min(total as u64) as usize;
                Ok((total as u64, vec![(read.1)(from, to)]))
            }),
            stats: Box::new(|| Ok(json!({"tables": []}))),
            idle: Duration::ZERO,
        }
    };
    let server = kermodes_gateway::serve(
        session.clone(),
        "127.0.0.1:0".parse().unwrap(),
        None,
        None,
        Some(stored),
    )
    .unwrap();
    let url = format!("ws://{}/ws", server.addr());
    let (mut socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    while text(&mut socket).await["tables"][0]["name"] != "md.ticks" {}
    let openings = || live.lock().unwrap().len();

    send(
        &mut socket,
        json!({"type": "open", "view": 1, "table": "md.ticks"}),
    )
    .await;
    let (state, header, rows, keys) = window(&mut socket).await;
    assert_eq!(
        (header["start"].clone(), state["row_count"].clone()),
        (json!(0), json!(250))
    );
    assert_eq!((syms(&rows)[0], keys[0], keys.len()), ("r0", 0, 200));
    assert_eq!(openings(), 0, "shown by position, nothing opened");

    // The table grows: the window follows within a poll.
    *day.lock().unwrap() = 260;
    let (state, ..) = window(&mut socket).await;
    assert_eq!(state["row_count"], 260);

    // Deep in the table: the rows there, keyed by their positions.
    send(
        &mut socket,
        json!({"type": "viewport", "view": 1, "top": 150, "bottom": 159}),
    )
    .await;
    let (_, header, rows, keys) = window(&mut socket).await;
    assert_eq!(header["start"], 50);
    assert_eq!((syms(&rows)[0], keys[0], keys.len()), ("r50", 50, 210));

    // Sorting opens the table live; the sorted rows show where the grid is.
    send(
        &mut socket,
        json!({"type": "sort", "view": 1, "request": 1,
               "by": [{"column": "price", "descending": true}]}),
    )
    .await;
    let (state, ..) = window(&mut socket).await;
    assert_eq!(
        (state["sort"][0]["column"].clone(), state["applied"].clone()),
        (json!("price"), json!(1))
    );
    assert_eq!(openings(), 1);
    // The live table loads in a cycle, as the store's feed gives it.
    session.step();
    let (_, header, rows, _) = window(&mut socket).await;
    assert_eq!(
        (header["start"].clone(), syms(&rows)[0]),
        (json!(50), "r209")
    );
    assert_eq!(openings(), 1);

    // Unsorted again: the window by position, and the live table let go.
    send(
        &mut socket,
        json!({"type": "sort", "view": 1, "request": 2, "by": []}),
    )
    .await;
    let (state, header, rows, _) = window(&mut socket).await;
    assert_eq!(
        (state["sort"].clone(), state["applied"].clone()),
        (json!([]), json!(2))
    );
    assert_eq!(
        (header["start"].clone(), syms(&rows)[0]),
        (json!(50), "r50")
    );
    assert!(
        live.lock().unwrap()[0].upgrade().is_none(),
        "the live table is let go"
    );
}
