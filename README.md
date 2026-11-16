# Kermodes

Cloud-native data platform where live and historical data share one table model: a real-time
incremental engine, a columnar ingest log (weir), a Parquet lake, scheduled jobs, a model registry,
a feature store, and an agent-first control surface. Built on Arrow, Arrow Flight, and Parquet.
User projects live in git and reach the cluster through a build-once, promote-everywhere pipeline.

Docs: [reecetec.github.io/kermodes](https://reecetec.github.io/kermodes/) (also as Markdown for
agents: [llms.txt](https://reecetec.github.io/kermodes/llms.txt)), from [docs/](docs/):
[architecture](docs/architecture.md), [engine](docs/engine.md), [theme](docs/theme.md).

```python
import pyarrow as pa
import kermodes as km

s = km.local(manual=True)  # cycles run on step(); km.local() runs them on input
ticks = s.input_table({"sym": km.string(), "price": km.float64(), "size": km.int64()})
live = ticks.where("price > 0").update("notional = price * size")

ticks.append(pa.table({"sym": ["a", "b"], "price": [1.5, -2.0], "size": [10, 20]}))
s.step()
pa.table(live)  # sym, price, size, notional for rows with price > 0
```

By default `km.local()` runs a cycle as soon as writes arrive, and a ticking table displayed in a
notebook updates in place; [examples/live.ipynb](examples/live.ipynb) streams a random-walk ticker.

## Development

Requires Rust and [uv](https://docs.astral.sh/uv/).

```sh
uv sync                    # builds the engine into .venv; reruns rebuild after Rust changes
uv run pytest
cargo test --workspace
uv run --group docs zensical serve   # the docs site at http://localhost:8000, rebuilt on save
```

The CI gate is `.github/workflows/ci.yml`; releases are `.github/workflows/release.yml`
(architecture.md §16.1). The docs site is `.github/workflows/docs.yml`: built on every change to
docs/, failing on a broken link, and published to GitHub Pages from master.
