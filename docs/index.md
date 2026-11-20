# Kermodes

A cloud-native data platform where live and historical data share one table model: a real-time
incremental engine, a columnar ingest log (weir), a Parquet lake, scheduled jobs, and apps with
live dashboards. Built on Arrow, Arrow Flight, and Parquet. Projects live in git and reach the
cluster through a build-once, promote-everywhere pipeline.

```python
import pyarrow as pa
import kermodes as km

s = km.connect(":memory:", manual=True)  # no store; cycles run on step(), else on input
ticks = s.input_table({"sym": km.string(), "price": km.float64(), "size": km.int64()})
live = ticks.where("price > 0").update("notional = price * size")

ticks.append(pa.table({"sym": ["a", "b"], "price": [1.5, -2.0], "size": [10, 20]}))
s.step()
pa.table(live)  # sym, price, size, notional for rows with price > 0
```

Install with `pip install kermodes`. A complete project, with a feed, an app, and a dashboard of
tables and live charts, is [kermodes-examples](https://github.com/reecetec/kermodes-examples):
`kermodes dev --project ticker` runs it locally.

## Reading on

| Page | What it covers |
|---|---|
| [Architecture](architecture.md) | The platform: ingest, the lake, the catalog, projects and apps, the web UI, charts, and how it runs in Kubernetes and locally |
| [Engine](engine.md) | The incremental engine: tables, cycles, operators, and how ticking results stay equal to their static recomputation |
| [Theme](theme.md) | The visual design: tokens, type, layout, live data states, and charts |

These pages are also served for agents as Markdown: [llms.txt](llms.txt) lists them, and each
page has a Markdown copy.
