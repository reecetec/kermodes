import urllib.error
import urllib.request

import ursid as us


def test_a_server_holds_published_tables_until_it_stops():
    s = us.local(manual=True)
    src = s.input_table({"sym": us.string(), "size": us.int64()})
    server = s.serve()
    assert server.url.startswith("http://127.0.0.1:")

    server.publish("latest", src.last_by("sym"))
    assert server.published == ["latest"]
    # Published, so the server keeps it although Python no longer refers to it.
    assert s.table_count == 2
    assert not server.unpublish("nope")

    # The server answers on its address (the app itself is built from web/).
    try:
        status = urllib.request.urlopen(server.url, timeout=10).status
    except urllib.error.HTTPError as e:
        status = e.code
    assert status in (200, 404)

    server.stop()
    assert server.published == []
    assert s.table_count == 1
