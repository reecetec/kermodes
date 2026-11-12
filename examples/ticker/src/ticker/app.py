"""md.ticks: a synthetic market-data feed, generated and ingested by one deployment, so it needs
nothing else running: `kermodes dev --project examples/ticker` locally, or synced from git by
`kermodes control` in the cluster."""

import time

import kermodes as km

COLUMNS = {
    "sym": km.string(),
    "price": km.float64(),
    "size": km.int64(),
    "ts": km.timestamp(),
}


# The feed and its connector use about 35m CPU and 40Mi at 1,500 ticks a second.
@km.deployment(cpu=0.25, memory="256Mi")
def ticks(db):
    db.create_table("md.ticks", COLUMNS)
    feed = km.demo.ticker(rate=1_500, symbols=100)
    # Waits for the run it replaces to hand over md.ticks (a rolling update).
    ingest = db.ingest_websocket(feed.url, "md.ticks", source="ticks", wait=True)
    # The feed and its connector run as long as this function holds them.
    while not ingest.stats.failed:
        time.sleep(1)
    raise km.KermodesError(f"md.ticks stopped: {ingest.stats.last_error}")
