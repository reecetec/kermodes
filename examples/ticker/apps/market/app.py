"""The market app's tables, computed in its own process and served to its page (App.tsx)."""

import kermodes as km
from ticker.md import COLUMNS, TICKS


# Its process holds today's md.ticks for the board: about 500 MiB at 8 million ticks.
@km.app(memory="2Gi")
def market(db):
    # Whichever starts first creates md.ticks (creating it again changes nothing).
    db.create_table(TICKS, COLUMNS)
    ticks = db.live_table(TICKS)
    day = ticks.agg_by(
        "sym",
        trades="count(*)",
        volume="sum(size)",
        low="min(price)",
        high="max(price)",
    )
    last = ticks.last_by("sym").select("sym", "price")
    return {"board": last.natural_join(day, on="sym")}
