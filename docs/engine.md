# Ursid engine (M0)

Direction for the first milestone: the engine as a library, driven from Python on `us.local()`,
with ticking tables in Jupyter. Like `architecture.md`, this is the current best guess; the code
corrects it.

## 1. Scope

In M0:

- Tables, row sets, deltas, the update graph, and both operator paths (static and incremental).
- Input tables: append-only and keyed (upsert/delete by key).
- Operators: `where`, `select` / `update`, `last_by`, `agg_by`, `natural_join`, `as_of_join`,
  `rolling`, `merge`, `head` / `tail`, static `sort`.
- Expressions in SQL syntax, parsed with `sqlparser`, compiled to arrow-rs kernels.
- Python API through PyO3, a Jupyter widget, the oracle test harness, and benchmarks.

Not in M0: weir, Flight, Parquet, full SQL queries, ticking `sort`, `predict`, multi-threaded
cycles, memory budget enforcement.

## 2. Data model

### 2.1 Types

`bool`, `int32`, `int64`, `float64`, `utf8`, `date32`, `timestamp[ns, UTC]`. Nulls everywhere.
Creating a table with any other column type fails. More types (decimal, dictionary, lists,
`utf8_view`) when a slice needs them.

### 2.2 Tables

A table is a schema, a row set, column sources, and attributes:

| Attribute | Meaning | Lets operators |
|---|---|---|
| `static` | Never changes | Run once, materialize, keep no state |
| `add_only` | Only `added` deltas | Skip removal state (group row sets, min/max multisets) |
| `append_only` | `add_only`, and new keys are greater than all existing keys | Use positions as keys; windows look only backward |
| `blink` | Rows visible for one cycle only | Aggregate without retaining rows |

Operators derive their output's attributes from their inputs' (e.g. `where` over `append_only` is
`append_only`; `last_by` output is never `add_only` unless the input is static). An operator that
needs an attribute its input lacks is rejected at plan time with an error saying which.

### 2.3 Row keys and row sets

- Every row has a `RowKey(u64)`. A key identifies one row for the lifetime of the table and is never
  reused. Keys are identities, not positions: there are no shifts.
- Append-only sources use the global row position as the key.
- State tables (`last_by`, `agg_by` outputs) use a slot number per group, assigned on first sight;
  keyed inputs use one per key value, mapped through the same grouping table (§5).
- `merge` packs `(input index, input key)` into the key: input index in the top 8 bits.
- Tables index their keys densely when append-only (key = position, located by binary search
  over chunk starts) and in a hash map otherwise; only snapshots need key order, and they sort.
  Deltas carry key vectors aligned with their row batches. A range-based row set arrives with
  viewports if a measurement calls for it.
- Display order is separate from keys: a viewport asks for positions, which are ranks in the row
  set (or in a sort order, later).

### 2.4 Storage

Append-only tables keep the batches they receive as immutable Arrow chunks, keyed by position,
and hand them out without copying.

Every other table (keyed inputs, `where` over them, `last_by` and `agg_by` outputs) gives each
live row a slot in mutable columns, with a hash map from key to slot:

- An addition takes a free slot or a new one.
- A modification overwrites the row's slot in place.
- A removal frees the slot for the next addition.

Memory follows the live rows, not the history of writes. Strings of up to 12 bytes live in the
slot (Arrow's string-view layout); longer ones in a per-column byte buffer, rewritten once its
superseded bytes outweigh the live ones (and exceed 1 MiB). Deltas carry previous values as
copies taken before a cycle is applied, so overwriting loses nothing.

Every operator materializes its output this way, including `where` (Arrow `filter`), `last_by`
(the group's last row is copied), and `agg_by`. Redirected views over a parent's rows and
coalescing small append-only chunks are later optimizations, taken when measurements call for
them.

Reading rows out produces Arrow `RecordBatch`es: the chunks themselves for append-only tables
(`interleave` for selected rows), and a gather from the slot columns for everything else.

## 3. Deltas

One delta per table per cycle, carrying data:

```
Delta { added: [(keys, rows)], removed: [(keys, last rows)], modified: [(keys, previous, current)] }
```

- `added`, `removed`, `modified` are disjoint. `removed` keys are gone after the cycle; `added`
  keys are new; `modified` keys keep their identity.
- Every column of a modified row counts as modified. Per-column change sets arrive when a
  benchmark shows re-evaluating unchanged columns costs enough to matter.
- A delta is observed exactly once per cycle by each child, during that cycle.
- Keyed inputs net a cycle's writes per key: the last write wins, and a key added and deleted in
  the same cycle never appears.

## 4. Update graph

- One graph per process session; every refreshing table belongs to it. Tables run in creation
  order, which is topological because a table is built only from existing tables; each consumes
  its parents' deltas for the cycle and produces its own.
- Sources accept writes from any thread into a queue. A cycle starts by draining the queues into
  the source tables, then propagates.
- One `RwLock` guards graph state: a cycle holds the write lock; snapshot readers hold the read
  lock between cycles. Readers copy what they need and release it.
- Clocks:
  - on input (the default for `us.local()`): a background thread runs a cycle as soon as writes
    arrive; writes that arrive during a cycle fold into the next, so batches grow with load and
    latency stays near zero when idle. `start(min_period)` optionally spaces cycles at least that
    far apart; `stop()` ends it. The thread holds the session weakly, so dropping the session
    ends it.
  - manual (`us.local(manual=True)`): `step()` runs one cycle (tests, replay).
- Each table records the cycle that last changed its rows (or failed it); viewers poll that to
  re-render only what changed.
- Cycles run tables one after another. `agg_by` spreads a large delta (64k+ rows) over every core
  in two phases (§5), or over `URSID_THREADS` cores if that is set; other operators are
  single-threaded until a profile shows the need.
- Failure: if an operator errors, its table and everything downstream of it enter a failed state
  holding the error; the rest of the graph keeps running. Snapshots of a failed table raise the
  error in Python.
- Building a table on a refreshing parent takes the read lock, computes the initial state from a
  snapshot, and registers the node so it receives deltas from the next cycle.

## 5. Operators

Each operator has a static path (inputs `static`: stream chunks once, materialize Arrow output,
keep no state) and an incremental path below. Static paths use the same kernels and hash tables.

| Operator | Incremental state | Incremental algorithm | M0 limits |
|---|---|---|---|
| `where(expr)` | Over an `add_only` input: none, and the output numbers its own rows. Otherwise the output keeps the input's keys. | Added: evaluate, keep matches. Modified: re-evaluate; true→true is modified, false→true added, true→false removed. Removed: drop if present. | — |
| `update(exprs)` / `select(exprs)` | None; same keys as the input | Added and modified: evaluate on the new values. Removed: removed. | — |
| `last_by(keys)` | Group arena; per group, its newest row key (`add_only` input) or the set of its row keys | Groups touched in the cycle compare their current last key with the one published; new group → added, emptied group → removed, different last or a modified last row → modified (the row is copied from the input). | Order is by row key (arrival); `last_by` over a `ts` order comes with ticking sort. |
| `agg_by(keys, aggs)` | Groups sharded by key hash, one shard per core; per shard, per-group accumulator states (one struct per group per aggregation) and (unless the input is `add_only`) each group's row keys | Two phases, as in DuckDB. Added rows are cut into fixed 256k-row pieces; each piece is grouped and folded on its own into partial states, reading rows sequentially and hashing each key once. Each shard then merges the partials of its groups in piece order (sums add; `std` combines Welford states with Chan et al.'s formula). Removed and modified rows are routed to their group's shard and taken back out: counts and sums subtract (a float sum restarts at exactly zero when its group empties), and `std` reverses Welford's update. `min`/`max`/`first`/`last` cannot subtract, so a group whose winning row leaves is recomputed from its rows in key order; any other row leaving changes nothing. `first`/`last` take the smallest/largest row key, and `min`/`max` break ties by the smaller row key, so they are exact in any order. Float `sum`/`mean`/`std` round differently depending on where piece boundaries fall, so ticking and static results can differ in the last bits; boundaries never depend on core count. Empty groups are removed. Output row order is unspecified, as in SQL `GROUP BY`. | Recomputing a group reads its rows by key, so heavy churn of `min`/`max`/`first`/`last` winners in large groups is slow. |
| `natural_join(right, on, joins)` | One grouping table for key values from both sides (NULL matches NULL, as in `agg_by`); per group its right rows and, only if the right side can change, its left rows (appended in key order when the left side only gains rows, else a sorted set) | Right changes first: a key with more than one right row fails the table. Then the left rows already joined whose key's right row changed are collected; then left removes, modifies, and adds are applied, looked up against the new right rows; finally the collected rows, minus those the left side removed or modified, are emitted as modified. Output rows keep the left keys and only gain rows when the left side only gains rows and the right side is static. | — |
| `as_of_join(right, on, at, joins)` | The same grouping table; per group, right rows by `(time, row key)`, and left rows by `(time, row key)` only if the right side ticks | A left row matches the right row with the greatest time not after its own, the latest arrival among equal times; NULL times never match. A right row added at time `t` re-matches exactly the left rows timed from `t` up to the group's next right time. Cycles order work as for `natural_join`. | Right must only gain rows; `at` is an integer, date, or timestamp column. |
| `rolling(by, window, aggs)` | Per group: a ring buffer (row windows) or deque (time windows) with running accumulators | Added rows in key order: push, evict out-of-window, emit the aggregate as columns on the added row. Earlier rows never change. | Input must be `append_only`; windows look backward only. |
| `merge(tables)` | None | Re-key each input's delta with its input index; union. | Same schema required. |
| `head(n)` / `tail(n)` | Output row set | Positions over the parent's row set; recompute the boundary on deltas. | — |
| `sort(by)` | — | Static only in M0. | Ticking sort is M2 (SQL `ORDER BY`). |

Grouping keys:

- Each key is packed into fixed-width words: integers directly, strings and row-encoded keys of
  up to 16 bytes exactly (first and last bytes loaded with overlapping fixed-size reads), longer
  ones by their first and last 8 bytes plus length. Piece-local tables store the packed key in
  the entry, so a probe compares in place and only keys over 16 bytes are compared in full. The
  hash comes from the packed words (the full bytes for long keys) with `ahash` under fixed seeds,
  computed once per row and reused for shard routing, piece tables, and shard tables. Groups are
  addressed by dense ids, and emptied groups free their id.
- Multi-column and other key types go through `arrow::row::RowConverter`'s comparable encoding.

## 6. Expressions

- Parse with `sqlparser` (`Parser::parse_expr`, generic dialect) for `where` and `update`
  strings; `update` strings are `name = <expr>`.
- Bind column names against the input schema, then type-check to a typed expression tree. Errors
  carry the expression text, position, and a fix hint (unknown column → closest names; type
  mismatch → the cast that would work).
- Compile to a tree of arrow-rs kernel calls evaluated per chunk: arithmetic (`arrow::compute`
  numeric kernels), comparison (`cmp`), boolean with SQL three-valued logic (`and_kleene`,
  `or_kleene`), `CASE` via `zip`, `CAST` via `cast`, `IN` via comparison chains or a hash set for
  long lists, `IS [NOT] NULL` from validity.
- Literals become scalars, never broadcast arrays. Constant subtrees fold at compile time.
- Integer division by zero yields null; overflow on checked arithmetic fails the table.
- M0 functions: `abs`, `round`, `floor`, `ceil`, `sqrt`, `ln`, `exp`, `power`, `least`,
  `greatest`, `coalesce`, `lower`, `upper`, `length`, `substring`, `now()`, `today()`,
  `date_trunc`.
- Later: the binder records which input columns an expression reads, so `where` and `update`
  skip modified rows whose changed columns the expression doesn't read.

## 7. Python API (M0)

```python
import ursid as us

s = us.local()  # in-process engine; cycles run as soon as writes arrive

ticks = s.input_table(
    {
        "sym": us.string(),
        "price": us.float64(),
        "size": us.int64(),
        "ts": us.timestamp(),
    }
)
refs = s.input_table({"sym": us.string(), "sector": us.string()}, key="sym")

ticks.append(batch)  # anything with __arrow_c_stream__ or __arrow_c_array__
refs.upsert(batch)
refs.delete(keys)  # key columns only

live = ticks.where("price > 0").update("notional = price * size")
latest = live.last_by("sym")
stats = live.agg_by("sym", n="count(*)", notional="sum(notional)", size="sum(size)")
vwap = stats.update("vwap = notional / size")

pa.table(latest)  # snapshot through the Arrow PyCapsule interface
latest  # in a notebook: a live view (§8)

m = us.local(manual=True)  # only m.step() runs cycles; m.start() / m.stop()

enriched = live.natural_join(refs, on="sym")  # `joins=` picks and renames right columns
priced = ticks.as_of_join(quotes, on="sym", at="ts")

# Still to come in M0:
vol = ticks.rolling("sym", rows=100, std_price="std(price)")
```

- Every engine call releases the interpreter while Rust works.
- Table methods build plan nodes and attach them to the graph immediately (eager, like
  Deephaven); a plan-only form arrives with the Ursid plan in M2.
- Type stubs for `_native` are hand-written; the public API is the Python layer in
  `python/ursid`.

## 8. Notebook view

- Displaying a table shows its first 20 rows in key order as HTML, with column types and the row
  count, rendered in Rust (the wheel has no Python dependencies).
- A ticking table's output is re-rendered in place through Jupyter's `update_display_data`: one
  background thread checks every live output ten times a second and re-renders those whose
  table changed since. This works in any frontend that honours display updates (Jupyter, VS
  Code, Tern), with no front-end code; `examples/live.ipynb` shows it.
- Scrolling past the first rows needs a widget (anywidget: the front end subscribes to a viewport
  and receives Arrow IPC for the visible rows). That comes when a slice needs it, in frontends
  that support widget comms.

## 9. Testing

- **Oracle:** for every operator and for random compositions, a seeded generator produces update
  sequences (appends; upserts and deletes on keyed inputs; small key spaces to force collisions;
  random nulls). After every cycle, each incremental table is compared with its static path run on
  snapshots of the same inputs, as sets of rows (order matters only for `head`/`tail`). Float
  columns compare to a relative 1e-9, since partial aggregation rounds by piece; everything else
  compares exactly. A failure prints the seed and the cycle that diverged.
- **Expressions:** decision tables per kernel family (null semantics, overflow, casts), and the
  same expressions checked against pandas in Python tests.
- **Python:** pytest over the public API on `us.local(manual=True)`, stepping manually.
- **Determinism:** no test depends on wall-clock time or thread scheduling.

## 10. Benchmarks

A benchmark binary in the workspace (`ursid-bench`: profiling-friendly ticker and order-book
workloads) and comparison scripts under `bench/`, all reading the same seeded data
(`bench/data.py`):

- `compare.py`: every static operator in Ursid, DuckDB, and Polars, all cores or `--threads N`.
  Each (engine, operator) runs in its own process, measuring time (best of three) and memory:
  RSS the engine adds when loading its inputs, the peak RSS during the operator (sampled every
  2 ms), and the RSS retained with its result alive. Row counts and checksums must agree across
  engines.
- `deephaven_compare.py`: the same operators in Deephaven Community, memory as JVM heap in use
  after a full collection.
- `ticking.py` / `deephaven_ticking.py`: each operator ticking alone over 100 cycles of 100k
  appended rows; cycle time p50/p99 and memory growth. Deephaven cycles are timed until its update
  graph finishes them (its exclusive lock), not until `add` returns.
- `deephaven_book.py`: the `ursid-bench book` workload in Deephaven.

Python comparisons need a release build of Ursid (`maturin develop --release`); `uv sync` builds
a faster-to-compile profile. Numbers go in commit messages with the machine; nothing is checked in
as a baseline file.

Targets (release build, on the development laptop):

| Workload | Target |
|---|---|
| Random walk, 10k symbols, 1M rows/s appended in 100k-row batches every 100 ms, through `where` → `update` (2 expressions) → `last_by(sym)` and `agg_by(sym)` (`count`, `mean`, `std`), for 60 s | p99 cycle < 50 ms; memory grows with source data only, plus O(groups) state |
| Same workload in Deephaven Community on the same machine | Ursid sustained throughput ≥ Deephaven's |
| Keyed order book (`ursid-bench book`): 1M orders over 100 symbols, 100k upserts and deletes per cycle, through `agg_by(sym)` (`count`, `sum`, `mean`, `std`); the same in Deephaven (`bench/deephaven_book.py`) | Cycle time ≤ Deephaven's |
| Static `agg_by` over 100M rows, 10k `int64` groups, `sum`/`mean`/`std` of a `float64` | Within 2× of Polars `group_by` on the same data |
| `import ursid; us.local()` | < 100 ms |

## 11. Code layout for M0

- `crates/ursid-core`: types, row sets, column sources, deltas, update graph, operators,
  expressions. One crate until a boundary or compile times justify splitting it.
- `crates/ursid-py`: the PyO3 module `ursid._native`.
- `crates/ursid-bench`: the benchmark binary.
- `python/ursid`: the Python API and widget. `cli.py` arrives with the first real command
  (`ursid dev`, M1).

## 12. What M0 will tell us

- Whether range-list row sets hold up under `where` on random data, or need a bitmap form.
- Whether single-threaded cycles meet the cycle target, and where parallelism pays first.
- Whether redirected columns (no copies) beat materializing for `last_by` and joins when read by
  viewports and downstream operators.
- The real cost of the Python boundary per call and per batch.
