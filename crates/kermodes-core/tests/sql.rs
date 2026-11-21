//! SQL (kermodes_core::sql): what a query binds to, and the oracle: each query built over
//! ticking tables equals the same query built over static copies of them, after every cycle of
//! seeded writes.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{
    ArrayRef, BooleanArray, Float64Array, Int64Array, RecordBatch, StringArray,
    TimestampNanosecondArray,
};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
use arrow::util::display::{ArrayFormatter, FormatOptions};
use kermodes_core::sql::{Plan, Scan, parse};
use kermodes_core::{Error, Session, Table};

/// The plan of `sql` as `EXPLAIN` prints it, its scans not described.
fn plan(sql: &str) -> String {
    let parsed = parse(sql).unwrap();
    parsed
        .plan
        .explain(&mut |_: &Scan| Ok((String::new(), false)))
        .unwrap()
        .lines()
        .map(|line| line.trim_start_matches("static ").trim_start().to_owned())
        .collect::<Vec<_>>()
        .join("\n")
}

#[test]
fn a_query_binds_to_the_operators_the_python_api_has() {
    let cases = [
        (
            // Every condition of a one-table query goes with its scan, for pruning
            "SELECT sym, price * size AS notional FROM md.ticks \
             WHERE Date = '2026-11-15' AND price > 0",
            "select sym, notional\n\
             update notional = price * size\n\
             scan md.ticks where (Date = '2026-11-15') AND (price > 0)",
        ),
        (
            // Aggregates under names of their own, expressions over them computed after;
            // `HAVING` before the result's columns, which take DuckDB's names
            "SELECT sym, count(*), sum(price * size) / sum(size) AS vwap FROM t \
             GROUP BY sym HAVING count(*) > 2",
            "select sym, count_star(), vwap\n\
             update \"count_star()\" = __a0, vwap = __a1 / __a2\n\
             where __a0 > 2\n\
             agg_by [sym]: __a0=count(*), __a1=sum(__e0), __a2=sum(size)\n\
             update __e0 = price * size\n\
             scan t",
        ),
        (
            // Only the first table's conditions go with a scan: a join keeps all its rows. The
            // join adds only the right table's columns the query reads, as `r.c`.
            "SELECT o.sym, o.px, r.sector FROM live.md.odds o \
             LEFT JOIN ref.syms r ON o.sym = r.sym \
             WHERE o.px > 1 AND r.sector = 'tech'",
            "select sym, px, sector\n\
             update sector = \"r.sector\"\n\
             where \"r.sector\" = 'tech'\n\
             natural_join on sym adding r.sector=sector\n\
             scan live.md.odds where px > 1\n\
             scan ref.syms",
        ),
        (
            "SELECT * FROM a ASOF JOIN b MATCH_CONDITION (a.ts >= b.at) ON a.k = b.key",
            "as_of_join on k=key at ts=at\n  scan a\n  scan b",
        ),
        (
            // A sorted result's rows by position are a slice: a ticking sort is read in order
            "WITH up AS (SELECT * FROM t WHERE px > 0) \
             SELECT sym FROM up UNION ALL SELECT sym FROM hist.md.s ORDER BY sym DESC LIMIT 5",
            "slice 0..5\n\
             sort sym desc\n\
             merge\n\
             select sym\n\
             scan t where px > 0\n\
             select sym\n\
             scan hist.md.s",
        ),
        (
            // Expressions grouped by are computed first; a result column that is one reads it
            "SELECT ClientIP, ClientIP - 1, count(*) AS c FROM t \
             GROUP BY ClientIP, ClientIP - 1 ORDER BY c DESC LIMIT 10 OFFSET 5",
            "slice 5..15\n\
             sort c desc\n\
             select ClientIP, (ClientIP - 1), c\n\
             update \"(ClientIP - 1)\" = __g1, c = __a0\n\
             agg_by [ClientIP, __g1]: __a0=count(*)\n\
             update __g1 = ClientIP - 1\n\
             scan t",
        ),
        (
            // GROUP BY an alias and a position; ORDER BY an aggregate and a position
            "SELECT extract(minute FROM ts) AS m, sym, count(*) FROM t GROUP BY m, 2 \
             ORDER BY count(*) DESC, 1",
            "sort count_star() desc, m\n\
             select m, sym, count_star()\n\
             update m = __g0, \"count_star()\" = __a0\n\
             agg_by [__g0, sym]: __a0=count(*)\n\
             update __g0 = EXTRACT(MINUTE FROM ts)\n\
             scan t",
        ),
        (
            // ORDER BY a column the result does not show: kept until sorted, then dropped
            "SELECT sym FROM t WHERE px > 0 ORDER BY ts LIMIT 3",
            "drop ts\n\
             slice 0..3\n\
             sort ts\n\
             select sym, ts\n\
             scan t where px > 0",
        ),
        (
            // ...and an aggregate the result does not show
            "SELECT sym, count(*) AS n FROM t GROUP BY sym ORDER BY sum(size)",
            "drop __o0\n\
             slice 0..\n\
             sort __o0\n\
             select sym, n, __o0\n\
             update __o0 = __a1, n = __a0\n\
             agg_by [sym]: __a0=count(*), __a1=sum(size)\n\
             scan t",
        ),
        (
            "SELECT sym, count(DISTINCT px) FROM t GROUP BY 1",
            "select sym, count(DISTINCT px)\n\
             update \"count(DISTINCT px)\" = __a0\n\
             agg_by [sym]: __a0=count_distinct(px)\n\
             scan t",
        ),
    ];
    for (sql, want) in cases {
        // The tree's indentation aside, the operators in order
        let got: Vec<String> = plan(sql).lines().map(|l| l.trim().to_owned()).collect();
        let want: Vec<String> = want.lines().map(|l| l.trim().to_owned()).collect();
        assert_eq!(got, want, "{sql}");
    }
}

#[test]
fn what_sql_cannot_say_yet_is_refused_by_name() {
    for (sql, names) in [
        (
            "SELECT a FROM t1 INNER JOIN t2 ON t1.k = t2.k",
            "INNER JOIN",
        ),
        ("SELECT a FROM t1, t2", "JOIN ... ON"),
        ("SELECT DISTINCT a FROM t", "DISTINCT"),
        ("SELECT a, sum(b) FROM t GROUP BY c", "neither grouped"),
        ("SELECT a, count(*) FROM t GROUP BY 3", "columns 1 to 2"),
        ("SELECT count(*) FROM t GROUP BY count(*)", "aggregate"),
        ("SELECT a FROM t ORDER BY 2", "columns 1 to 1"),
        ("SELECT * FROM t ORDER BY 1", "SELECT *"),
        ("SELECT a + 1, a + 1 FROM t", "two columns named (a + 1)"),
        ("SELECT sum(DISTINCT a) AS s FROM t", "DISTINCT"),
        ("SELECT a FROM t UNION SELECT a FROM u", "UNION ALL"),
        (
            "SELECT a FROM t UNION ALL SELECT a FROM u ORDER BY a + 1",
            "order a UNION",
        ),
        (
            "SELECT * FROM a ASOF JOIN b MATCH_CONDITION (a.ts > b.ts)",
            "left.ts >= right.ts",
        ),
        ("SELECT sum(a) OVER () AS s FROM t", "window functions"),
        ("DELETE FROM t", "queries only"),
    ] {
        let error = parse(sql).map(|_| ()).unwrap_err().to_string();
        assert!(error.contains(names), "{sql}: {error}");
    }
    assert!(parse("EXPLAIN SELECT a FROM t").unwrap().explain);
}

/// A name `GROUP BY` reads as both a column of the table and an alias in `SELECT` is the column
/// in SQL; refused rather than grouped by the alias.
#[test]
fn group_by_a_name_that_is_a_column_and_an_alias_is_refused() {
    let session = Session::new();
    let t = session.static_table(ticks_schema(), vec![]).unwrap();
    let tables = HashMap::from([("t", t)]);
    let error = build(
        "SELECT size + 1 AS price, count(*) FROM t GROUP BY price",
        &tables,
    )
    .map(|_| ())
    .unwrap_err()
    .to_string();
    assert!(error.contains("both a column"), "{error}");
    build(
        "SELECT size + 1 AS more, count(*) FROM t GROUP BY more",
        &tables,
    )
    .unwrap();
}

/// Each scan names the columns the query may read of its table, so a store reads only those:
/// every column it names bare (which table holds it is the tables' to say) or by that table's
/// name, join keys included; all of them after `*`.
#[test]
fn a_scan_names_the_columns_the_query_reads() {
    fn scans(plan: &Plan, out: &mut Vec<Scan>) {
        match plan {
            Plan::Scan(scan) => out.push(scan.clone()),
            Plan::Filter { input, .. }
            | Plan::Update { input, .. }
            | Plan::Select { input, .. }
            | Plan::Agg { input, .. }
            | Plan::Sort { input, .. }
            | Plan::Head { input, .. }
            | Plan::Slice { input, .. }
            | Plan::Drop { input, .. } => scans(input, out),
            Plan::Join { left, right, .. } => {
                scans(left, out);
                scans(right, out);
            }
            Plan::Merge(parts) => parts.iter().for_each(|p| scans(p, out)),
        }
    }
    let columns = |sql: &str| {
        let mut out = Vec::new();
        scans(&parse(sql).unwrap().plan, &mut out);
        out.into_iter()
            .map(|scan| (scan.name, scan.columns))
            .collect::<Vec<_>>()
    };
    let names = |names: &[&str]| Some(names.iter().map(|n| n.to_string()).collect::<Vec<_>>());
    assert_eq!(
        columns(
            "SELECT sym, count(*) FROM t WHERE px > 0 GROUP BY sym HAVING max(ts) > 1 \
             ORDER BY sum(size)"
        ),
        [("t".to_owned(), names(&["px", "size", "sym", "ts"]))]
    );
    assert_eq!(
        columns("SELECT * FROM t WHERE px > 0"),
        [("t".to_owned(), None)]
    );
    assert_eq!(
        columns(
            "SELECT o.px, r.sector FROM a o LEFT JOIN b r ON o.sym = r.sym \
             ASOF JOIN c m MATCH_CONDITION (o.ts >= m.at) ON o.k = m.k"
        ),
        [
            ("a".to_owned(), names(&["k", "px", "sym", "ts"])),
            ("b".to_owned(), names(&["sector", "sym"])),
            ("c".to_owned(), names(&["at", "k"])),
        ]
    );
}

struct Rng(u64);

impl Rng {
    fn below(&mut self, n: u64) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0 % n
    }
}

fn ticks_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", DataType::Int64, false),
        Field::new("sym", DataType::Utf8, true),
        Field::new("price", DataType::Float64, true),
        Field::new("size", DataType::Int64, true),
        Field::new("flag", DataType::Boolean, true),
        Field::new(
            "ts",
            DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
            false,
        ),
    ]))
}

fn sectors_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("sym", DataType::Utf8, false),
        Field::new("sector", DataType::Utf8, true),
        Field::new("weight", DataType::Int64, true),
    ]))
}

const SYMS: [&str; 4] = ["a", "b", "c", "d"];

/// Rows with ids from a small range (so keyed writes collide), whole-number prices (so sums are
/// exact in any order), and rising times.
fn ticks(rng: &mut Rng, rows: usize, clock: &mut i64) -> RecordBatch {
    let mut columns = (
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
        Vec::new(),
    );
    for _ in 0..rows {
        *clock += 1 + rng.below(3) as i64;
        columns.0.push(rng.below(20) as i64);
        columns
            .1
            .push((rng.below(10) != 0).then(|| SYMS[rng.below(4) as usize]));
        columns
            .2
            .push((rng.below(10) != 0).then(|| rng.below(200) as f64 - 50.0));
        columns
            .3
            .push((rng.below(10) != 0).then(|| rng.below(13) as i64));
        columns
            .4
            .push((rng.below(10) != 0).then(|| rng.below(2) == 0));
        columns.5.push(*clock);
    }
    RecordBatch::try_new(
        ticks_schema(),
        vec![
            Arc::new(Int64Array::from(columns.0)) as ArrayRef,
            Arc::new(StringArray::from(columns.1)),
            Arc::new(Float64Array::from(columns.2)),
            Arc::new(Int64Array::from(columns.3)),
            Arc::new(BooleanArray::from(columns.4)),
            Arc::new(TimestampNanosecondArray::from(columns.5).with_timezone("UTC")),
        ],
    )
    .unwrap()
}

fn sector_rows(rng: &mut Rng) -> RecordBatch {
    let syms: Vec<&str> = SYMS.iter().copied().filter(|_| rng.below(4) != 0).collect();
    let sector: Vec<Option<&str>> = syms
        .iter()
        .map(|_| (rng.below(5) != 0).then(|| ["tech", "energy"][rng.below(2) as usize]))
        .collect();
    let weight: Vec<i64> = syms.iter().map(|_| rng.below(9) as i64).collect();
    RecordBatch::try_new(
        sectors_schema(),
        vec![
            Arc::new(StringArray::from(syms)) as ArrayRef,
            Arc::new(StringArray::from(sector)),
            Arc::new(Int64Array::from(weight)),
        ],
    )
    .unwrap()
}

fn ids(rng: &mut Rng, rows: usize) -> RecordBatch {
    let ids: Vec<i64> = (0..rows).map(|_| rng.below(20) as i64).collect();
    RecordBatch::try_from_iter([("id", Arc::new(Int64Array::from(ids)) as ArrayRef)]).unwrap()
}

/// A table's rows as sorted text, so tables compare as sets of rows.
fn contents(table: &Table) -> Vec<Vec<String>> {
    let all = concat_batches(table.schema(), &table.snapshot().unwrap()).unwrap();
    let options = FormatOptions::default().with_null("∅");
    let formatters: Vec<_> = all
        .columns()
        .iter()
        .map(|c| ArrayFormatter::try_new(c.as_ref(), &options).unwrap())
        .collect();
    let mut rows: Vec<Vec<String>> = (0..all.num_rows())
        .map(|r| formatters.iter().map(|f| f.value(r).to_string()).collect())
        .collect();
    rows.sort();
    rows
}

/// Builds `sql` over `tables`, each scan's predicate applied to its table.
fn build(sql: &str, tables: &HashMap<&str, Table>) -> Result<Table, Error> {
    parse(sql)?.plan.build(&mut |scan: &Scan| {
        let table = tables[scan.name.as_str()].clone();
        match &scan.predicate {
            Some(predicate) => table.filter(predicate),
            None => Ok(table),
        }
    })
}

/// Queries over the keyed `book` (upserted and deleted) and `sectors`.
const BOOK_QUERIES: &[&str] = &[
    "SELECT sym, price * size AS notional, flag FROM book WHERE price > 0 AND flag",
    "SELECT * FROM book WHERE sym IS NOT NULL",
    "SELECT sym, count(*), sum(size) AS total, max(price) AS high, avg(price) AS mean_px \
     FROM book GROUP BY sym",
    "SELECT flag, sum(price * size) AS value, count(*) AS n FROM book \
     GROUP BY flag HAVING count(*) > 2",
    "SELECT count(*) AS n, min(size) AS low FROM book WHERE size > 3",
    "SELECT b.id, b.sym, b.price, s.sector, s.weight FROM book b \
     LEFT JOIN sectors s ON b.sym = s.sym WHERE b.price > -20",
    "SELECT sector, sum(size) AS total FROM \
     (SELECT b.size, s.sector FROM book b LEFT JOIN sectors s ON b.sym = s.sym) \
     GROUP BY sector",
    "WITH big AS (SELECT id, sym, price FROM book WHERE size > 6) \
     SELECT sym, price FROM big UNION ALL SELECT sym, price FROM book WHERE size < 2",
    // Grouped by an expression and by position; ordered by an aggregate, then a key, so the
    // rows each position holds are one
    "SELECT size - 1, count(*) AS n FROM book GROUP BY size - 1",
    "SELECT sym, count(*), sum(size) FROM book GROUP BY 1 \
     ORDER BY sum(size) DESC, sym LIMIT 2 OFFSET 1",
    // Ordered by columns the result does not show, with and without a limit
    "SELECT id, price FROM book WHERE price > 0 ORDER BY size, id LIMIT 5 OFFSET 2",
    "SELECT price FROM book ORDER BY id",
    "SELECT sym, count(DISTINCT size) AS kinds, count(*) FROM book GROUP BY sym",
];

/// Queries over the appended `tape` and its as-of partner `marks`.
const TAPE_QUERIES: &[&str] = &[
    "SELECT t.sym, t.price, m.price AS mark FROM tape t \
     ASOF JOIN marks m MATCH_CONDITION (t.ts >= m.ts) ON t.sym = m.sym",
    "SELECT sym, last(price) AS last_px, first(price) AS first_px, count(*) FROM tape GROUP BY sym",
];

#[test]
fn every_query_ticks_to_what_it_says_over_the_tables_as_they_are() {
    for seed in 1..=40 {
        let mut rng = Rng(seed * 104_729);
        let session = Session::new();
        let book = session.keyed_input_table(&ticks_schema(), &["id"]).unwrap();
        let sectors = session
            .keyed_input_table(&sectors_schema(), &["sym"])
            .unwrap();
        let tape = session.input_table(&ticks_schema()).unwrap();
        let marks = session.input_table(&ticks_schema()).unwrap();
        let live: HashMap<&str, Table> = HashMap::from([
            ("book", book.clone()),
            ("sectors", sectors.clone()),
            ("tape", tape.clone()),
            ("marks", marks.clone()),
        ]);
        let queries: Vec<(&str, Table)> = BOOK_QUERIES
            .iter()
            .chain(TAPE_QUERIES)
            .map(|sql| (*sql, build(sql, &live).unwrap()))
            .collect();
        let mut clock = 1_767_225_600_000_000_000;
        for cycle in 0..10 {
            for _ in 0..1 + rng.below(3) {
                match rng.below(4) {
                    0 => {
                        let rows = rng.below(6) as usize;
                        book.delete(&[ids(&mut rng, rows)]).unwrap();
                    }
                    _ => {
                        let rows = rng.below(12) as usize;
                        book.upsert(&[ticks(&mut rng, rows, &mut clock)]).unwrap();
                    }
                }
            }
            if cycle % 3 == 0 {
                sectors.upsert(&[sector_rows(&mut rng)]).unwrap();
            }
            let rows = rng.below(10) as usize;
            tape.append(&[ticks(&mut rng, rows, &mut clock)]).unwrap();
            let rows = rng.below(4) as usize;
            marks.append(&[ticks(&mut rng, rows, &mut clock)]).unwrap();
            session.step();
            // The static path: the same query over a static copy of each table as it is now
            let fixed = Session::new();
            let copies: HashMap<&str, Table> = live
                .iter()
                .map(|(name, table)| {
                    let rows = table.snapshot().unwrap();
                    (
                        *name,
                        fixed.static_table(table.schema().clone(), rows).unwrap(),
                    )
                })
                .collect();
            for (sql, ticking) in &queries {
                let want = build(sql, &copies).unwrap();
                assert_eq!(
                    contents(ticking),
                    contents(&want),
                    "seed {seed} cycle {cycle}: {sql}"
                );
            }
        }
    }
}
