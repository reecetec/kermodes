"""ClickBench's 43 queries (https://github.com/ClickHouse/ClickBench) in Kermodes and DuckDB: which
run, how long each takes, and whether Kermodes's results are right, DuckDB's being the oracle.

    uv run --no-sync --with maturin maturin develop --release -m crates/kermodes-py/Cargo.toml
    uv run --no-sync --with duckdb python bench/clickbench.py              # hits_0: 1M rows
    uv run --no-sync --with duckdb python bench/clickbench.py --files 100  # all 100M rows
    ... python bench/clickbench.py --threads 1 --only 13,33

Use a release build of Kermodes: `uv sync` installs a faster-to-build profile.

The data is ClickBench's `hits` as 100 Parquet files of about 1M rows each; `--files N` takes the
first N. It and the queries are downloaded from a pinned ClickBench commit into
`.kermodes/clickbench/` (ClickBench is CC BY-NC-SA, so none of it is checked in here), with
ClickBench's conversions: byte columns as strings, `EventDate` (days) as a date, `EventTime`
(seconds) as a timestamp.

Each engine loads the data into its own storage once, reused by later runs (`--reload` loads it
again): Kermodes into the history of a store's `bench.hits`, a `write_history` per date, sorted
by ClickHouse's key for the table (`CounterID, UserID, EventTime, WatchID`; its date is
history's); DuckDB into a database file. Kermodes reads it as `bench.hits`. Each query runs
three times; the best is reported.

Each engine runs in its own process. A result is right if every row of it is in DuckDB's result
of the query without its `LIMIT` and `OFFSET`, at a position its `ORDER BY` allows: rows tied on
the `ORDER BY` may come in any order, and a query without `ORDER BY` may return any of its rows.
Checking needs DuckDB's whole result of each query, so `--check` is on only for small runs.
"""

import argparse
import json
import math
import re
import shutil
import subprocess
import sys
import time
import urllib.request
from datetime import date
from decimal import Decimal
from pathlib import Path

CLICKBENCH = "https://raw.githubusercontent.com/ClickHouse/ClickBench/88971bb1833be72dbc5ff9ebfb54a0a19e721370"
HITS = (
    "https://datasets.clickhouse.com/hits_compatible/athena_partitioned/hits_{}.parquet"
)
DIR = Path(__file__).resolve().parent.parent / ".kermodes" / "clickbench"
REPEATS = 3
SORT = ["CounterID", "UserID", "EventTime", "WatchID"]


def fetch(url: str, path: Path) -> Path:
    if not path.exists():
        print(f"downloading {url}", file=sys.stderr, flush=True)
        part = path.with_suffix(".part")
        # The dataset host refuses urllib's default User-Agent.
        request = urllib.request.Request(url, headers={"User-Agent": "kermodes-bench"})
        with urllib.request.urlopen(request) as response, part.open("wb") as out:
            shutil.copyfileobj(response, out)
        part.rename(path)
    return path


def queries() -> list[str]:
    text = fetch(f"{CLICKBENCH}/duckdb/queries.sql", DIR / "queries.sql").read_text()
    return [q.strip().rstrip(";") for q in text.splitlines() if q.strip()]


def hits(files: int) -> list[Path]:
    return [fetch(HITS.format(i), DIR / f"hits_{i}.parquet") for i in range(files)]


def converted(table):
    """`hits` as ClickBench's DuckDB setup reads it (module docs)."""
    import pyarrow as pa
    import pyarrow.compute as pc

    columns = []
    for field, column in zip(table.schema, table.columns, strict=True):
        if field.name == "EventDate":
            column = pc.cast(pc.cast(column, pa.int32()), pa.date32())
        elif field.name == "EventTime":
            column = pc.cast(
                pc.multiply(column, 1_000_000_000), pa.timestamp("ns", tz="UTC")
            )
        elif pa.types.is_binary(field.type):
            column = pc.cast(column, pa.utf8())
        columns.append(column)
    return pa.Table.from_arrays(columns, names=table.schema.names)


def kermodes(files: list[Path], wanted: list[int], reload: bool) -> dict:
    from datetime import timedelta

    import kermodes as km
    import pyarrow as pa
    import pyarrow.dataset as ds

    store = DIR / f"store-{len(files)}"
    loaded = store / "loaded"
    load = None
    if reload or not loaded.exists():
        shutil.rmtree(store, ignore_errors=True)
        start = time.perf_counter()
        db = km.connect(store, manual=True)
        data = ds.dataset([str(p) for p in files], format="parquet")
        sample = converted(data.head(1))
        names = {pa.utf8(): "utf8", pa.date32(): "date32"}
        db.create_table(
            "bench.hits",
            {
                f.name: "timestamp"
                if pa.types.is_timestamp(f.type)
                else names.get(f.type, str(f.type))
                for f in sample.schema
            },
            sort=SORT,
        )
        days = data.to_table(columns=["EventDate"]).column(0).unique().to_pylist()
        for day in sorted(days):
            rows = converted(data.to_table(filter=ds.field("EventDate") == day))
            on = (date(1970, 1, 1) + timedelta(days=day)).isoformat()
            db.write_history("bench.hits", on, rows)
        load = time.perf_counter() - start
        del db
        loaded.touch()
    db = km.connect(store, manual=True)

    def query(q: str) -> pa.Table:
        return pa.table(db.sql(re.sub(r"\bFROM hits\b", "FROM bench.hits", q)))

    return run(wanted, load, query)


def duckdb_engine(
    files: list[Path], wanted: list[int], threads: int | None, reload: bool
) -> dict:
    import duckdb

    path = DIR / f"hits-{len(files)}.duckdb"
    load = None
    if reload or not path.exists():
        path.unlink(missing_ok=True)
        start = time.perf_counter()
        db = duckdb.connect(path)
        paths = ", ".join(f"'{p}'" for p in files)
        db.execute(
            f"""CREATE TABLE hits AS SELECT * REPLACE (make_date(EventDate) AS EventDate,
            to_timestamp(EventTime) AS EventTime)
            FROM read_parquet([{paths}], binary_as_string = true)"""
        )
        db.close()
        load = time.perf_counter() - start
    db = duckdb.connect(path)
    db.execute("SET TimeZone = 'UTC'")
    if threads:
        db.execute(f"SET threads = {threads}")
    return run(wanted, load, lambda q: db.sql(q).to_arrow_table())


def run(wanted: list[int], load: float, query) -> dict:
    """Each wanted query's best time of three and its result's rows, or its error."""
    import pyarrow as pa

    all_queries = queries()
    out = {"load": load, "queries": {}}
    for n in wanted:
        try:
            times = []
            for _ in range(REPEATS):
                start = time.perf_counter()
                result: pa.Table = query(all_queries[n - 1])
                times.append(time.perf_counter() - start)
            out["queries"][n] = {
                "best": min(times),
                "first": times[0],
                "rows": rows(result),
            }
        except Exception as e:  # noqa: BLE001 - an error is the query's result here
            out["queries"][n] = {"error": str(e).splitlines()[0]}
        print(f"  q{n} done", file=sys.stderr, flush=True)
    return out


def value(v):
    """A value as compared across engines: DuckDB's exact sums are decimals; floats are compared
    to 15 significant digits, what float64 holds of any exact result; dates and times as ISO text."""
    if isinstance(v, Decimal):
        return int(v) if v == v.to_integral_value() else float(v)
    if isinstance(v, float) and math.isfinite(v):
        return float(f"{v:.15g}")
    if isinstance(v, date):
        return v.isoformat()
    return v


def rows(table) -> list[list]:
    return [
        [value(v) for v in row]
        for row in zip(*(c.to_pylist() for c in table.columns), strict=True)
    ]


def ranked(query: str) -> str | None:
    """The query with each row's rank by its `ORDER BY` and no `LIMIT`/`OFFSET`: the rows any
    right answer may take each position from. None for a query without `ORDER BY`. The keys are
    computed in the query as extra columns (a window cannot name the query's aliases), then
    ranked outside it."""
    found = re.search(r"\sORDER BY (.+?)(?:\sLIMIT\s.*)?$", query, re.IGNORECASE)
    if not found:
        return None
    keys, depth, start = [], 0, 0
    clause = found.group(1)
    for i, ch in enumerate(clause + ","):
        depth += {"(": 1, ")": -1}.get(ch, 0)
        if ch == "," and depth == 0:
            keys.append(clause[start:i].strip())
            start = i + 1
    columns, order = [], []
    for i, key in enumerate(keys):
        expression, descending = re.subn(r"\s+DESC$", "", key, flags=re.IGNORECASE)
        expression = re.sub(r"\s+ASC$", "", expression, flags=re.IGNORECASE)
        columns.append(f"{expression} AS __k{i}")
        order.append(f"__k{i}{' DESC' if descending else ''}")
    # The query's own FROM, not one inside a call such as `extract(minute FROM t)`.
    depth, start = 0, None
    for match in re.finditer(r"[()]|\sFROM\s", query, re.IGNORECASE):
        token = match.group()
        depth += {"(": 1, ")": -1}.get(token, 0)
        if token not in "()" and depth == 0:
            start = match.start()
            break
    assert start is not None, query
    inner = f"{query[:start]}, {', '.join(columns)}{query[start : found.start()]}"
    hidden = ", ".join(f"__k{i}" for i in range(len(keys)))
    return (
        f"SELECT rank() OVER (ORDER BY {', '.join(order)}) AS __rank, "
        f"* EXCLUDE ({hidden}) FROM ({inner})"
    )


def window(query: str) -> tuple[int, int | None]:
    limit = re.search(r"\sLIMIT (\d+)(?: OFFSET (\d+))?$", query, re.IGNORECASE)
    if not limit:
        return 0, None
    return int(limit.group(2) or 0), int(limit.group(1))


def check(files: list[Path], wanted: list[int], ours: dict) -> dict[int, str]:
    """Whether each of Kermodes's results is one a right answer could be (module docs)."""
    import duckdb

    # The table DuckDB's worker loaded.
    db = duckdb.connect(DIR / f"hits-{len(files)}.duckdb", read_only=True)
    db.execute("SET TimeZone = 'UTC'")
    all_queries = queries()
    verdicts = {}
    for n in wanted:
        result = ours["queries"][str(n)]
        if "error" in result:
            continue
        query = all_queries[n - 1]
        offset, limit = window(query)
        got = [tuple(map(repr, row)) for row in result["rows"]]
        order = ranked(query)
        if order is None:
            whole = re.sub(r"\sLIMIT \d+$", "", query, flags=re.IGNORECASE)
            every = {
                tuple(map(repr, row)) for row in rows(db.sql(whole).to_arrow_table())
            }
            expected = len(every) if limit is None else min(limit, len(every))
            missing = [row for row in got if row not in every]
            verdicts[n] = (
                f"wrong: {len(got)} rows, expected {expected}"
                if len(got) != expected
                else f"wrong: {len(missing)} rows not in the result"
                if missing
                else "ok"
            )
            continue
        table = db.sql(order).to_arrow_table()
        ranks = table.column("__rank").to_pylist()
        rest = rows(table.drop_columns(["__rank"]))
        # Each row's rank, and how many rows share it: positions rank-1 .. rank-1+ties-1.
        ties: dict[int, int] = {}
        for r in ranks:
            ties[r] = ties.get(r, 0) + 1
        places: dict[tuple, list[int]] = {}
        for r, row in zip(ranks, rest, strict=True):
            places.setdefault(tuple(map(repr, row)), []).append(r)
        expected = (
            max(0, len(rest) - offset)
            if limit is None
            else max(0, min(limit, len(rest) - offset))
        )
        wrong = None
        if len(got) != expected:
            wrong = f"{len(got)} rows, expected {expected}"
        for i, row in enumerate(got):
            position = offset + i
            if wrong is None and not any(
                r - 1 <= position < r - 1 + ties[r] for r in places.get(row, [])
            ):
                wrong = f"row {i + 1} cannot be there: {list(row)[:6]}"
        verdicts[n] = f"wrong: {wrong}" if wrong else "ok"
    return verdicts


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--files", type=int, default=1)
    parser.add_argument("--threads", type=int)
    parser.add_argument("--only", help="query numbers, comma-separated (from 1)")
    parser.add_argument(
        "--check",
        action=argparse.BooleanOptionalAction,
        help="default: on for up to 10 files",
    )
    parser.add_argument(
        "--reload",
        action="store_true",
        help="load the data again, not as stored before",
    )
    parser.add_argument(
        "--worker", choices=["kermodes", "duckdb"], help=argparse.SUPPRESS
    )
    args = parser.parse_args()
    DIR.mkdir(parents=True, exist_ok=True)
    files = hits(args.files)
    wanted = (
        [int(n) for n in args.only.split(",")]
        if args.only
        else list(range(1, len(queries()) + 1))
    )
    if args.worker == "kermodes":
        print(json.dumps(kermodes(files, wanted, args.reload)))
        return
    if args.worker == "duckdb":
        print(json.dumps(duckdb_engine(files, wanted, args.threads, args.reload)))
        return

    import os

    env = dict(os.environ)
    if args.threads:
        env["KERMODES_THREADS"] = str(args.threads)
    measured = {}
    for engine in ("kermodes", "duckdb"):
        print(f"{engine}:", file=sys.stderr, flush=True)
        argv = [
            sys.executable,
            __file__,
            "--worker",
            engine,
            "--files",
            str(args.files),
        ]
        argv += ["--only", ",".join(map(str, wanted))] + (
            ["--threads", str(args.threads)] if args.threads else []
        )
        argv += ["--reload"] if args.reload else []
        done = subprocess.run(
            argv, env=env, capture_output=True, text=True, check=False
        )
        if done.returncode != 0:
            sys.exit(f"{engine} failed:\n{done.stderr}")
        measured[engine] = json.loads(done.stdout)
    ours, theirs = measured["kermodes"], measured["duckdb"]
    checking = args.check if args.check is not None else args.files <= 10
    verdicts = check(files, wanted, ours) if checking else {}

    def loaded(seconds: float | None) -> str:
        return "as stored before" if seconds is None else f"{seconds:.2f} s"

    print(
        f"{len(files)} file(s); load: Kermodes {loaded(ours['load'])}, "
        f"DuckDB {loaded(theirs['load'])}\n"
    )
    print("| Q | Kermodes ms | DuckDB ms | Ratio | Result |")
    print("|---|---|---|---|---|")
    ratios = []
    for n in wanted:
        k, d = ours["queries"][str(n)], theirs["queries"][str(n)]
        if "error" in k:
            print(f"| {n} | | {d['best'] * 1000:.1f} | | error: {k['error'][:120]} |")
            continue
        ratio = k["best"] / d["best"]
        ratios.append(ratio)
        print(
            f"| {n} | {k['best'] * 1000:.1f} | {d['best'] * 1000:.1f} | {ratio:.2f}× | {verdicts.get(n, 'unchecked')} |"
        )
    ran = len(ratios)
    right = sum(1 for v in verdicts.values() if v == "ok")
    print(f"\nran {ran} of {len(wanted)}", end="")
    if verdicts:
        print(f"; right {right} of {ran}", end="")
    if ratios:
        mean = math.exp(sum(math.log(r) for r in ratios) / ran)
        print(f"; geometric mean {mean:.2f}× DuckDB, worst {max(ratios):.2f}×", end="")
    print()


if __name__ == "__main__":
    main()
