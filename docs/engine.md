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
More types (decimal, dictionary, lists, `utf8_view`) when a slice needs them.

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
- State tables (`last_by`, `agg_by` outputs) use a slot number per group, assigned on first sight.
- `merge` packs `(input index, input key)` into the key: input index in the top 8 bits.
- `RowSet` is a sorted list of disjoint key ranges with rank/select (position ↔ key) by
  binary search over cumulative range lengths. An append-only table is one range. If fragmented
  row sets measure badly, the representation changes behind the type.
- Display order is separate from keys: a viewport asks for positions, which are ranks in the row
  set (or in a sort order, later).

### 2.4 Column sources

| Source | Used by | Storage |
|---|---|---|
| Chunked | Append-only inputs; computed columns over append-only parents | Immutable Arrow arrays in chunks; key → (chunk, offset) by binary search over chunk starts. Small appends are coalesced to ~64Ki-row chunks at cycle end. |
| Slot | State tables, keyed inputs, computed columns over them | Mutable typed vectors plus validity bitmap, indexed by slot; mutated in place. |
| Redirected | `last_by`, `natural_join`, `as_of_join` outputs | A slot → parent row key map; values are read from the parent's columns, never copied. |
| Filtered | `where` | M0 materializes the matching rows with Arrow's `filter`; a view over the parent replaces it only if memory measurements call for it. |

Reading rows out (snapshots, viewports, operator inputs) produces Arrow `RecordBatch`es: zero-copy
slices for contiguous chunk ranges, `take` for everything else.

## 3. Deltas

One delta per table per cycle:

```
Update { added: RowSet, removed: RowSet, modified: RowSet, modified_columns: ColumnSet }
```

- `added`, `removed`, `modified` are disjoint. `removed` keys are gone after the cycle; `added`
  keys are new; `modified` keys keep their identity with new values in `modified_columns`.
- `modified_columns` is per table delta, not per row. Operators recompute only expressions whose
  inputs intersect it and propagate the narrowest set they can.
- A delta is observed exactly once per cycle by each child, during that cycle.

## 4. Update graph

- One graph per process session; every refreshing table belongs to it. Tables run in creation
  order, which is topological because a table is built only from existing tables; each consumes
  its parents' deltas for the cycle and produces its own.
- Sources accept writes from any thread into a queue. A cycle starts by draining the queues into
  the source tables, then propagates.
- One `RwLock` guards graph state: a cycle holds the write lock; snapshot readers hold the read
  lock between cycles. Readers copy what they need and release it.
- Clocks:
  - manual: `step()` runs one cycle (tests, notebooks, replay);
  - periodic: a background thread runs a cycle every period (default 100 ms) when a source has
    pending data.
- Single-threaded cycles in M0. Parallelism inside operators (over chunks) comes when a benchmark
  shows the need.
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
| `where(expr)` | Output row set | Added: evaluate, keep matches. Modified (if `expr` reads a modified column): re-evaluate; true→true is modified, false→true added, true→false removed. Removed: drop. | — |
| `update(exprs)` / `select(exprs)` | Computed columns in the parent's key space | Added and modified (if inputs intersect `modified_columns`): evaluate and write. Removed: nothing. | — |
| `last_by(keys)` | Hash map group → slot; slot → parent row key (redirection); per-group row set only if the parent is not `add_only` | Added: the row becomes its group's last if its key is greater; new group → added slot, changed last → modified slot. Removed or modified last row: recompute the group's last from its row set. | Order is by row key (arrival); `last_by` over a `ts` order comes with ticking sort. |
| `agg_by(keys, aggs)` | Group map; per-group accumulators; per-group row set only if the parent is not `add_only` | `count`, `sum`, `mean`, `std` (Welford) are invertible: apply adds, subtract removes, remove-then-add for modifies. `min`, `max`, `first`, `last` apply adds directly; a remove or modify that touches the current extreme recomputes that group from its row set. Empty groups are removed. | — |
| `natural_join(right, on)` | Right: key → row key. Left: key → left row set. | Left added/modified-on-key: look up; output redirection to the right row. Right added/modified/removed for a key: every left row with that key is modified. A key matching more than one right row fails the table. | — |
| `as_of_join(right, on, ts)` | Right: per key, sorted `(ts, row key)`. Left: per key, sorted `(ts, row key)`. | Left added: binary search the greatest right `ts ≤ left ts`. Right added at `ts = t`: left rows with that key, `ts ≥ t`, and current match `< t` rematch (modified). | Right must be `add_only`. |
| `rolling(by, window, aggs)` | Per group: a ring buffer (row windows) or deque (time windows) with running accumulators | Added rows in key order: push, evict out-of-window, emit the aggregate as columns on the added row. Earlier rows never change. | Input must be `append_only`; windows look backward only. |
| `merge(tables)` | None | Re-key each input's delta with its input index; union. | Same schema required. |
| `head(n)` / `tail(n)` | Output row set | Positions over the parent's row set; recompute the boundary on deltas. | — |
| `sort(by)` | — | Static only in M0. | Ticking sort is M2 (SQL `ORDER BY`). |

Grouping keys:

- Single `int64`/`int32`/`date32`/`timestamp` key: `HashMap<i64, Slot>` fast path.
- Single `utf8` key: map over string bytes.
- Composite keys: `arrow::row::RowConverter` encodes the key columns to comparable bytes per row;
  the map is over those bytes.
- `hashbrown` with a fast non-cryptographic hasher (both already in the dependency tree via arrow
  and parquet).

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
- The binder records which input columns an expression reads; `where` and `update` use it to
  skip work when `modified_columns` doesn't intersect.

## 7. Python API (M0)

```python
import ursid as us

s = us.local()  # in-process engine and update graph

ticks = s.input_table(
    {
        "sym": us.string(),
        "price": us.float64(),
        "size": us.int64(),
        "ts": us.timestamp(),
    },
    kind="append",
)
refs = s.input_table(
    {"sym": us.string(), "sector": us.string()}, kind="keyed", key="sym"
)

ticks.append(
    batch
)  # anything with __arrow_c_stream__ / __arrow_c_array__, or a dict of lists
refs.upsert(batch)
refs.delete(keys)

live = (
    ticks.where("price > 0")
    .update("notional = price * size")
    .natural_join(refs, on="sym")
)
latest = live.last_by("sym")
stats = live.agg_by("sector", n="count(*)", vwap="sum(notional) / sum(size)")
vol = ticks.rolling("sym", rows=100, std_price="std(price)")

s.step()  # one cycle; or s.start(period="100ms") / s.stop()
latest.to_arrow()  # snapshot as an Arrow stream (PyCapsule); .to_pandas(), .to_polars()
latest  # in Jupyter: ticking widget

demo = us.sources.random_walk(
    s, symbols=10_000, rows_per_sec=1_000_000
)  # Rust-side source for demos and benchmarks
```

- Every engine call releases the interpreter while Rust works.
- Table methods build plan nodes and attach them to the graph immediately (eager, like
  Deephaven); a plan-only form arrives with the Ursid plan in M2.
- Type stubs for `_native` are hand-written; the public API is the Python layer in
  `python/ursid`.

## 8. Notebook widget

- An anywidget front end; the kernel side talks to the engine directly (no server in M0).
- Front end → kernel: `subscribe {first_row, last_row, columns}` when the viewport scrolls or
  resizes.
- Kernel → front end, at most 10 times a second and only when the viewport's rows or the row count
  changed: `{row_count, first_row, batch}` where `batch` is Arrow IPC bytes for the visible rows.
- The front end decodes with `apache-arrow` and renders with TanStack Virtual. The same message
  shapes are later carried over WebSocket by the gateway.

## 9. Testing

- **Oracle:** for every operator and for random compositions, a seeded generator produces update
  sequences (appends; upserts and deletes on keyed inputs; small key spaces to force collisions;
  random nulls). After every cycle, each incremental table is compared with its static path run on
  snapshots of the same inputs, as sets of rows (order matters only for `head`/`tail`). A failure
  prints the seed and the minimal cycle that diverged.
- **Expressions:** decision tables per kernel family (null semantics, overflow, casts), and the
  same expressions checked against pandas in Python tests.
- **Python:** pytest over the public API on `us.local()`, stepping manually.
- **Determinism:** no test depends on wall-clock time or thread scheduling.

## 10. Benchmarks

A benchmark binary in the workspace (`ursid-bench`) and Python comparison scripts under `bench/`.
Numbers go in commit messages with the machine; nothing is checked in as a baseline file.

Targets (release build, on the development laptop):

| Workload | Target |
|---|---|
| Random walk, 10k symbols, 1M rows/s appended in 100k-row batches every 100 ms, through `where` → `update` (2 expressions) → `last_by(sym)` and `agg_by(sym)` (`count`, `mean`, `std`), for 60 s | p99 cycle < 50 ms; memory grows with source data only, plus O(groups) state |
| Same workload in Deephaven Community on the same machine | Ursid sustained throughput ≥ Deephaven's |
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
