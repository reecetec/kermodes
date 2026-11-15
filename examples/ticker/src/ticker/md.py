"""md.ticks, which the ticks deployment writes and the market app reads."""

import kermodes as km

TICKS = "md.ticks"
COLUMNS = {
    "sym": km.string(),
    "price": km.float64(),
    "size": km.int64(),
    "ts": km.timestamp(),
}
