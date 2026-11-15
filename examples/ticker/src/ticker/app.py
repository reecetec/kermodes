"""md.ticks: a synthetic market-data feed, generated and ingested by one deployment, so it needs
nothing else running: `kermodes dev --project examples/ticker` locally, or synced from git by
`kermodes control` in the cluster."""

import time

import humanize
import kermodes as km

COLUMNS = {
    "sym": km.string(),
    "price": km.float64(),
    "size": km.int64(),
    "ts": km.timestamp(),
}


# The market app: this function runs in the app's own process, which serves the board it returns
# and the TypeScript page in apps/market. Its process holds today's md.ticks for the board.
@km.app(memory="2Gi")
def market(db):
    # Whichever starts first creates md.ticks (creating it again changes nothing).
    db.create_table("md.ticks", COLUMNS)
    ticks = db.live_table("md.ticks")
    day = ticks.agg_by(
        "sym",
        trades="count(*)",
        volume="sum(size)",
        low="min(price)",
        high="max(price)",
    )
    last = ticks.last_by("sym").select("sym", "price")
    return {"board": last.natural_join(day, on="sym")}


# About 100 ticks a second, some 100 MB a day in weir: a demo that runs for weeks until the merge
# job moves days to the lake. It uses well under its size (35m CPU and 40Mi at 1,500 a second).
@km.deployment(cpu=0.25, memory="256Mi")
def ticks(db):
    db.create_table("md.ticks", COLUMNS)
    rate = 100
    feed = km.demo.ticker(rate=rate, symbols=100)
    # humanize comes from the project's environment, built from its uv.lock.
    print(f"md.ticks: about {humanize.intword(rate * 86_400)} ticks a day", flush=True)
    # Waits for the run it replaces to hand over md.ticks (a rolling update).
    ingest = db.ingest_websocket(feed.url, "md.ticks", source="ticks", wait=True)
    # The feed and its connector run as long as this function holds them.
    while not ingest.stats.failed:
        time.sleep(1)
    raise km.KermodesError(f"md.ticks stopped: {ingest.stats.last_error}")
