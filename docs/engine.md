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
- Tables index their keys densely when append-only (key = position, located by binary search
  over chunk starts) and in a `BTreeMap` otherwise. Deltas carry key vectors aligned with their
  row batches. A range-based row set arrives with viewports if a measurement calls for it.
- Display order is separate from keys: a viewport asks for positions, which are ranks in the row
  set (or in a sort order, later).

### 2.4 Storage

Every table stores row versions in immutable Arrow chunks, with an index from key to the chunk
and row holding its current version:

- An addition appends a chunk.
- A modification appends the new version and re-points the key; the old version stays readable,
  so previous values for the cycle cost nothing extra.
- A removal drops the key from the index.

Every operator materializes its output this way, including `where` (Arrow `filter`), `last_by`
(the group's last row is copied), and `agg_by`. Redirected views over a parent's rows, slot
columns mutated in place, chunk coalescing, and compaction of superseded versions are later
optimizations, taken when memory measurements call for them.

Reading rows out produces Arrow `RecordBatch`es: the chunks themselves for append-only tables,
`interleave` for everything else.

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
  - manual: `step()` runs one cycle (tests, notebooks, replay);
  - periodic: a background thread runs a cycle every period (default 100 ms) when a source has
    pending data.
- Cycles run tables one after another. `agg_by` spreads a large delta (64k+ rows) over every core
  by sharding its groups (§5); other operators are single-threaded until a profile shows the
  need.
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
| `agg_by(keys, aggs)` | Groups sharded by key hash, one shard per core; per shard, per-group accumulators stored column-wise and (unless the input is `add_only`) each group's row keys | Each batch becomes a vector of group ids, then each aggregation folds the batch in one typed loop. Rows added in key order fold in; a removed or modified row, or an addition older than the group's newest row, marks the group stale, and stale groups are recomputed from their rows in key order. Folding always in key order keeps incremental results bit-identical to the static path, on any number of cores. `min`/`max`/`first`/`last` keep the winning row key and copy its value. Empty groups are removed. Output row order is unspecified, as in SQL `GROUP BY`. | Invertible updates (subtracting removed rows) come if recomputation shows up in a profile. |
| `natural_join(right, on)` | Right: key → row key. Left: key → left row set. | Left added/modified-on-key: look up; output redirection to the right row. Right added/modified/removed for a key: every left row with that key is modified. A key matching more than one right row fails the table. | — |
| `as_of_join(right, on, ts)` | Right: per key, sorted `(ts, row key)`. Left: per key, sorted `(ts, row key)`. | Left added: binary search the greatest right `ts ≤ left ts`. Right added at `ts = t`: left rows with that key, `ts ≥ t`, and current match `< t` rematch (modified). | Right must be `add_only`. |
| `rolling(by, window, aggs)` | Per group: a ring buffer (row windows) or deque (time windows) with running accumulators | Added rows in key order: push, evict out-of-window, emit the aggregate as columns on the added row. Earlier rows never change. | Input must be `append_only`; windows look backward only. |
| `merge(tables)` | None | Re-key each input's delta with its input index; union. | Same schema required. |
| `head(n)` / `tail(n)` | Output row set | Positions over the parent's row set; recompute the boundary on deltas. | — |
| `sort(by)` | — | Static only in M0. | Ticking sort is M2 (SQL `ORDER BY`). |

Grouping keys:

- A single integer, date, or timestamp key is hashed as an `i64`; a single string key as its
  bytes; anything else through `arrow::row::RowConverter`'s comparable encoding. Maps use
  `ahash` (already in the dependency tree through arrow-array). Groups are then addressed by
  dense ids, and emptied groups free their id.
- Shards: a delta's rows are hashed to shards in parallel 64k-row pieces; each shard then reads
  its rows in place through index lists. A sampling profile (`samply`) of a 10M-row static
  `agg_by` shows the shards' scattered reads of keys and values as the dominant cost.

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

s = us.local()  # in-process engine and update graph

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

s.step()  # one cycle
pa.table(latest)  # snapshot through the Arrow PyCapsule interface

# Still to come in M0:
enriched = live.natural_join(refs, on="sym")
vol = ticks.rolling("sym", rows=100, std_price="std(price)")
s.start(period="100ms")  # background cycles; s.stop()
latest  # in Jupyter: ticking widget
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
