# Ursid agent instructions

## Mission

Build the smallest complete Ursid that someone can actually use: ticking tables in a notebook first, then each layer the architecture describes, one working slice at a time.

`docs/architecture.md` is the direction and `docs/engine.md` the engine design. Both are ideals, not contracts: like driving at night, we see as far as the headlights and still make the trip. When the code teaches us something they got wrong, fix or delete that part of the doc in the same change. There are no decision logs or status documents; commit messages carry the why.

Prefer code that is easy to change or delete over code designed for imagined reuse. Simplicity must not erase incremental correctness, durability of ingested data, tenancy, or failure handling.

## Stack

Facts about what Ursid runs on. Changing one is a normal change, not an architecture change.

- One Cargo workspace (the Rust core) and one Python package, `ursid`, built with maturin; Rust is exposed as `ursid._native` through PyO3. The `ursid` command is `argparse` over it. One wheel and one image with process roles; roles are not microservices.
- Data path: the arrow-rs family (`arrow`, `arrow-flight`, `parquet`, `object_store`) and `sqlparser` for parsing. No embedded query engines. A new data-path dependency needs its reason in `docs/architecture.md` §4.1.
- Python tooling is uv. The web UI is React, Vite, Bun, and shadcn/ui.

## Rules

Build 0 to 1:

- Deliver the smallest complete vertical slice. An operator is done when it runs on the static and incremental paths, is reachable from Python, passes the oracle test, and has a benchmark number. A crate, trait, or module alone is not a feature.
- Implement the present requirement directly. No extension points for hypothetical requirements.
- First occurrence: write it. Second: duplication is acceptable. Third: extract only if the examples are the same stable concept.
- Prefer one obvious function or module over factories, registries, strategies, plugin systems, and configuration engines.
- A trait exists only for a real substitution (local vs Kubernetes runtime, SQLite vs Postgres), never for one implementation or merely to mock it.
- Crates split when they earn it (a real boundary or compile time), not up front.
- When behavior is replaced, delete the old path. Compatibility is kept only for known consumers: on-disk formats, the wire protocol, and released Python API.
- No unfinished work behind scaffolding, fallback mocks, TODOs, `todo!()`, or interfaces with no real implementation.
- Build a slice back to front and land its entry point (Python API, CLI command, UI route) last. No release toggles.

Design and refactoring, after [martinfowler.com](https://martinfowler.com/):

- Wear one hat at a time. A refactoring preserves behavior and keeps tests green at every step; a behavior change is a separate step. Refactor to make the next change easy, then make the easy change.
- Treat code smells as triggers, not style, and fix them with the named refactoring.
- Split Phase: separate the decision from the I/O. Plans, merge layouts, pruning, and scheduling policy are pure functions with unit tests; the code that touches files, sockets, and processes does what the decision says.
- Give recurring concepts one type and one parser at the boundary: row keys, column IDs, schema versions, `namespace.table` names, sequence numbers. Two `u64`s or strings that can be swapped are a bug waiting to compile.
- Parse at the boundary, trust inside. Flight descriptors, ingest batches, catalog rows, configuration, and environment variables are parsed once where they enter; behind that point the type is trusted.
- Command-query separation: a function that reads like a query does not mutate state.
- Tolerant Reader for connectors: read only the source fields you use, accept unknown additions, reject only what you cannot use. One bad record never discards a batch.
- Evolutionary formats: weir segments, the catalog schema, and protocol metadata are versioned, changes are forward-only, and cross-version changes use Parallel Change (expand, migrate, contract).
- Comments explain why the code is the way it is. Measurements and their dates belong in the commit message, not in comments or docs.
- Yagni and the design stamina hypothesis: pay for design where it makes the next change cheaper now.
- Code is cost; the capability is the asset. Deliver behavior with the smallest change that reads clearly and remove what it makes obsolete in the same change. Clearer beats shorter.

Engine:

- The static path is the oracle. Every incremental operator is tested against the static path on the same inputs after every cycle, under seeded random update sequences that include modifies and removes where the operator supports them.
- Determinism: the same inputs under the replay clock give the same outputs. Tests step the graph manually; no wall-clock time or thread timing in tests.
- Performance is a feature. Hot paths work on Arrow chunks, not rows: no per-row allocation, boxing, or dynamic dispatch, and no copy that a view, redirection, or PyCapsule can avoid. A claim about speed comes with a benchmark run, and the numbers and machine go in the commit message.
- `unsafe` only with a measured reason and a `// SAFETY:` comment stating the invariant it relies on.
- At the Python boundary, release the interpreter around engine work and move data as Arrow through the PyCapsule interface, never as Python objects per row.
- A failing expression fails its table and the tables downstream of it, never the process. Errors name the table, the column, and the position in the expression.

Tests:

- Self-testing code in a practical pyramid: decision tables as unit tests, the oracle tests above, and integration tests with real collaborators: real Arrow data, real Parquet files and weir segments in temporary directories, real Flight servers on localhost, real SQLite and Postgres.
- Test behavior through a module's public entry point. A pure refactoring must not require changing a test. No tests of wiring, private helpers, mock call order, or message wording.
- pandas and pyarrow are acceptable test oracles in development dependencies.

Delivery:

- Main stays green. Changes reach it through a pull request gated by CI; if main is red, nothing else lands until it is green.
- Build once and promote the same artifact. Never rebuild for a later target.
- No services, queues, or infrastructure beyond what `docs/architecture.md` names without a measured constraint.

## Worktrees

Each change happens in its own git worktree on a branch from `origin/main`. Local platform state lives in the worktree's `./.ursid/`, so worktrees never share weir segments, a lake, or a catalog.

## Verify for real

Tests passing is not evidence the change works. Run the changed path the way its user would and look at the result:

| Change | Verify by |
| --- | --- |
| An operator or expression | Build it from Python on `us.local()`, tick it, and compare the result with a pandas recomputation of the same input. |
| Ingest or weir | Run `ursid dev`, push data, kill the process with `-9`, restart, and check the table has no gaps or duplicates. |
| History, merge, or the catalog | Merge a day, read it back as `db.historical_table`, and check row counts and pruning in `EXPLAIN`. |
| SQL | Run the query once over merged dates (static) and once over today (ticking). |
| The UI | Open it in a browser against `ursid dev` and use the changed screen. |
| The Helm chart or controller | Deploy to the cluster through the Argo CD repo and watch the workers reach ready. |

The pull request says under **Tested for real** what you ran and what you saw. A path you could not run goes under **Not tested for real** with the reason; never imply the rest.

## Notebooks

Notebooks (`examples/` and research) are a person thinking in a REPL:

- Work in short turns: one or two cells, run them, show the result, stop.
- Straight-line, top to bottom, one step per cell. Parameters and imports in the first cells.
- Show the least that proves the step worked: a count, a shape, a few numbers.
- No defensive code; a traceback is the error message.
- Markdown for a trader: one line for what a step asks, one line for what it showed, numbers over adjectives.
- Code a second notebook needs moves into the package as its own change.

## Enforcement

`cargo fmt`, `cargo clippy -- -D warnings`, `cargo test`, `cargo deny` (one arrow-rs major, licenses), and for Python `ruff`, `ty`, and `pytest` are the gate. Unused code and dependencies are deleted, not silenced. Enforcement without a test is a wish.

> Name the concrete current constraint and the simpler option you rejected. If the defense is "best practice", "clean architecture", "scalability", or "flexibility", delete it.

> The user cannot use your crate structure. Ship the working path from the entry point through the engine to the data.
