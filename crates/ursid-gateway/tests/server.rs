//! The local server as the web app uses it: tables listed and opened over the WebSocket,
//! windows pushed after cycles, and the app's files.

use std::sync::Arc;
use std::time::Duration;

use arrow::array::{ArrayRef, AsArray, Float64Array, RecordBatch, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::reader::StreamReader;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};
use ursid_core::Session;

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
    let mut server =
        ursid_gateway::serve(session.clone(), "127.0.0.1:0".parse().unwrap(), None).unwrap();
    server.publish("book", &book);

    let url = format!("ws://{}/ws", server.addr());
    let (mut socket, _) = tokio_tungstenite::connect_async(url).await.unwrap();
    let tables = text(&mut socket).await;
    assert_eq!(tables["type"], "tables");
    assert_eq!(tables["tables"][0]["name"], "book");
    assert_eq!(tables["tables"][0]["refreshing"], true);
    assert_eq!(tables["tables"][0]["columns"][1]["type"], "float64");

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

    assert!(server.unpublish("book"));
    let tables = text(&mut socket).await;
    assert_eq!(tables["tables"], json!([]));
    server.stop();
}

#[tokio::test(flavor = "multi_thread")]
async fn the_app_is_served_from_its_directory_only() {
    let app = std::env::temp_dir().join(format!("ursid-gateway-app-{}", std::process::id()));
    std::fs::create_dir_all(app.join("assets")).unwrap();
    std::fs::write(app.join("index.html"), "<!doctype html>ursid").unwrap();
    std::fs::write(app.join("assets/app.js"), "export {}").unwrap();
    let server = ursid_gateway::serve(
        Session::new(),
        "127.0.0.1:0".parse().unwrap(),
        Some(app.clone()),
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
        index.contains("text/html") && index.ends_with("ursid"),
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
