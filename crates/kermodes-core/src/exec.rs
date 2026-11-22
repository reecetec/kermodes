//! The static executor (architecture.md §5.3, engine.md §4): a static plan over stored tables,
//! run outside any session's update graph, on its own threads, streaming batches. A table's rows
//! arrive as parts read in parallel, and results never depend on the number of threads: each
//! part's output is put back in part order.
//!
//! Operators that keep what they must, not the data: `where`, `update`, `select`, `drop`, and
//! merges run within each part as its batches stream through; a head or slice stops reading
//! once it has its rows, opening no later part; a tail keeps its last rows; an aggregation folds
//! each part into partial states merged in part order, so it holds its groups; a head or slice
//! of a sort keeps each part's best rows. The rest collect their input: a sort, and joins,
//! `last_by`, and `rolling`, which run the engine's own static operators on it in a session of
//! the query's own, so no caller's graph is held.
//!
//! Late materialization: over a source that reads its parts again by column and row
//! (`Reread`), a head or slice of a sort of the scan, and the scan's predicate where it reads
//! fewer than half the columns, read first only the columns they choose rows by, each row with
//! its position, and then the other columns of the rows they keep alone.
//!
//! The result equals `Plan::build`'s over static tables of the same rows: same schema, rows, and
//! order. Each node is also built over empty tables in that session, which gives its schema, and
//! its errors and description exactly as the engine words them.

use std::collections::BTreeSet;
use std::panic::{self, AssertUnwindSafe};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};
use std::thread;

use arrow::array::{
    Array, ArrayRef, AsArray, Int64Array, RecordBatchOptions, UInt64Array, new_null_array,
};
use arrow::compute::kernels::cmp;
use arrow::compute::{
    SortOptions, cast, concat, concat_batches, sort_to_indices, take, take_record_batch,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef, UInt64Type};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;

use crate::agg::{self, Fold};
use crate::expr::reads;
use crate::groups::resolve;
use crate::ops::Where;
use crate::parallel::{PIECE_ROWS, offsets, threads};
use crate::plan::{Plan, Scan, names};
use crate::session::{bind_select, bind_update};
use crate::sort::{gather, order, sorted};
use crate::storage::RowKey;
use crate::{Error, Expression, Session, Table};

/// A table's rows for a plan's scan, as parts read independently and in parallel, in the
/// table's order: part i's rows all come before part i+1's. A source that can read its parts
/// again by column and row says how (`reread`); one that knows what parts hold without reading
/// them, from statistics, says so (`stats`); each leaves what it cannot `None`.
pub struct Source {
    pub schema: SchemaRef,
    pub parts: Vec<Part>,
    pub reread: Option<Reread>,
    pub stats: Option<Stats>,
}

/// What part `part` of a source holds of its columns at `columns` (positions in its schema),
/// from statistics that hold exactly; `None` where the part has none. Called on any thread.
pub type Stats = Arc<dyn Fn(usize, &[usize]) -> Result<Option<PartStats>, Error> + Send + Sync>;

/// A part's rows as its statistics give them.
#[derive(Debug, Clone)]
pub struct PartStats {
    /// Its rows: exactly when `whole`, else at most.
    pub rows: u64,
    /// Whether the statistics are of exactly the rows the part reads, not of more (whole row
    /// groups whose pages a predicate skipped): `rows` and the columns' extremes are then the
    /// part's own, where otherwise they only bound them.
    pub whole: bool,
    /// Per column asked, its extremes and NULLs; `None` where not known.
    pub columns: Vec<Option<ColumnStats>>,
}

/// A column's least and greatest non-NULL values, as one-row arrays of its type (NULL where it
/// holds only NULLs), and its NULLs.
#[derive(Debug, Clone)]
pub struct ColumnStats {
    pub min: ArrayRef,
    pub max: ArrayRef,
    pub nulls: u64,
}

/// Reads one part: its batches in order. Opened on whichever thread runs it.
pub type Part = Box<dyn FnOnce() -> Result<Batches, Error> + Send>;

pub type Batches = Box<dyn Iterator<Item = Result<RecordBatch, Error>> + Send>;

/// Reads part `part` of a source again: only its columns at `columns` (positions in the
/// source's schema, ascending), as a schema of those, and only its rows at `rows` (positions
/// among the part's rows, ascending; all where `None`). A part's rows are the same, in the
/// same order, however it is read. Called on whichever thread runs it.
pub type Reread =
    Arc<dyn Fn(usize, &[usize], Option<&[u64]>) -> Result<Batches, Error> + Send + Sync>;

/// The plan's rows, computed outside any session's graph. `scan` gives each scan's `Source`,
/// honoring the scan as `Plan::build`'s scan does, except that its predicate is applied here.
pub fn execute(
    plan: &Plan,
    scan: &mut dyn FnMut(&Scan) -> Result<Source, Error>,
) -> Result<(SchemaRef, Vec<RecordBatch>), Error> {
    let mut executor = Executor {
        session: Session::new(),
        scan,
    };
    let node = executor.node(plan)?;
    let rows = collect(node.rows.into_parts()?)?;
    Ok((node.shape.schema().clone(), rows))
}

/// Bits of a row's position that number it within its part (an aggregation's row key, a row
/// a scan reads again); the part's index takes the rest, so positions follow scan order.
const PART_BITS: u32 = 40;

/// A row's place in its part, of its position.
const ROW: u64 = (1 << PART_BITS) - 1;

/// A batch-at-a-time operator: `where`, `update`, `select`.
type Map = Arc<dyn Fn(RecordBatch) -> Result<RecordBatch, Error> + Send + Sync>;

struct Executor<'a> {
    /// Holds the tables the plan is built from: empty ones for each node's shape, and the
    /// collected inputs of the engine's operators.
    session: Session,
    scan: &'a mut dyn FnMut(&Scan) -> Result<Source, Error>,
}

/// A plan node: its table over empty inputs, and its rows.
struct Node {
    shape: Table,
    rows: Rows,
}

enum Rows {
    Parts(Vec<Part>),
    /// A sort not yet done, so a head or slice over it keeps only its rows.
    Sorted(Sort),
    /// A scan whose source reads again, its predicate not yet applied, so what keeps few of its
    /// rows reads most columns for those alone.
    Scanned(Scanned),
}

struct Sort {
    input: Box<Rows>,
    schema: SchemaRef,
    keys: Vec<(usize, bool)>,
    /// The sort's description, for its errors.
    table: String,
    /// Operators over the sorted rows that keep each row in place, run once the rows are known.
    after: Vec<Map>,
}

struct Scanned {
    schema: SchemaRef,
    parts: Vec<Part>,
    reread: Reread,
    stats: Option<Stats>,
    predicate: Option<String>,
    /// The scan's description, for its errors.
    table: String,
}

/// What a narrow read of a scan's parts reads: some of its columns, then each row's position.
struct Narrow {
    /// The source's columns read, ascending.
    columns: Arc<[usize]>,
    /// Their schema, then the position's.
    schema: SchemaRef,
    /// The scan's predicate, over these columns.
    filter: Option<Map>,
}

impl Executor<'_> {
    fn node(&mut self, plan: &Plan) -> Result<Node, Error> {
        if let Plan::Scan(scan) = plan {
            return self.scan(scan);
        }
        let mut inputs = plan
            .inputs()
            .into_iter()
            .map(|input| self.node(input))
            .collect::<Result<Vec<_>, _>>()?;
        let shape = plan.apply(&inputs.iter().map(|n| &n.shape).collect::<Vec<_>>())?;
        let rows = match plan {
            Plan::LastBy { .. } | Plan::Rolling { .. } | Plan::Join { .. } => {
                self.engine(plan, inputs)?
            }
            Plan::Merge(_) => {
                let schema = shape.schema().clone();
                let table = shape.description();
                let rewrap: Map = Arc::new(move |batch: RecordBatch| {
                    batch
                        .with_schema(schema.clone())
                        .map_err(|e| failed(&table, e))
                });
                let mut parts = Vec::new();
                for input in inputs {
                    parts.extend(mapped(input.rows.into_parts()?, &rewrap));
                }
                Rows::Parts(parts)
            }
            _ => {
                let input = inputs.pop().expect("one input");
                let schema = input.shape.schema();
                match plan {
                    Plan::Filter { predicate, .. } => input
                        .rows
                        .then(filter(shape.description(), schema, predicate)?, false)?,
                    Plan::Update { assignments, .. } => {
                        let update = bind_update(schema, &names(assignments))?;
                        let table = shape.description();
                        let map: Map = Arc::new(move |batch| {
                            update.compute(&batch).map_err(|e| failed(&table, e))
                        });
                        input.rows.then(map, true)?
                    }
                    Plan::Select { .. } | Plan::Drop { .. } => {
                        // A drop is a select of the columns it keeps: its output's.
                        let columns: Vec<String> = match plan {
                            Plan::Select { columns, .. } => columns.clone(),
                            _ => shape
                                .schema()
                                .fields()
                                .iter()
                                .map(|f| f.name().clone())
                                .collect(),
                        };
                        let select = bind_select(schema, &names(&columns))?;
                        let table = shape.description();
                        let map: Map = Arc::new(move |batch| {
                            select.compute(&batch).map_err(|e| failed(&table, e))
                        });
                        // Columns by name alone, of a scan read again: still the scan's, read
                        // again and known by statistics through the selection.
                        let plain: Option<Vec<usize>> = columns
                            .iter()
                            .map(|name| schema.index_of(name).ok())
                            .collect();
                        match (input.rows, plain) {
                            (Rows::Scanned(scanned), Some(at)) if scanned.keeps(&at) => {
                                Rows::Scanned(scanned.selected(at, shape.schema().clone(), map))
                            }
                            (rows, _) => rows.then(map, true)?,
                        }
                    }
                    Plan::Agg {
                        by, aggregations, ..
                    } => {
                        if by.is_empty()
                            && let Rows::Scanned(scanned) = &input.rows
                            && let Some(batch) = scanned.summarized(aggregations, shape.schema())?
                        {
                            Rows::Parts(pieces(vec![batch]))
                        } else {
                            let specs: Vec<&str> =
                                aggregations.iter().map(|(_, spec)| spec.as_str()).collect();
                            let fold = Fold::new(schema, resolve(schema, &names(by))?, &specs)?;
                            let rows = aggregate(
                                input.rows.into_parts()?,
                                &fold,
                                shape.schema(),
                                &shape.description(),
                            )?;
                            Rows::Parts(pieces(rows))
                        }
                    }
                    Plan::Sort { by, .. } => {
                        let keys = by
                            .iter()
                            .map(|key| Ok((resolve(schema, &[&key.column])?[0], key.descending)))
                            .collect::<Result<Vec<_>, Error>>()?;
                        Rows::Sorted(Sort {
                            schema: schema.clone(),
                            input: Box::new(input.rows),
                            keys,
                            table: shape.description(),
                            after: Vec::new(),
                        })
                    }
                    Plan::Head { rows, .. } => input.rows.slice(0, Some(*rows))?,
                    Plan::Slice { start, stop, .. } => input.rows.slice(*start, *stop)?,
                    Plan::Tail { rows, .. } => {
                        Rows::Parts(pieces(tail(input.rows.into_parts()?, *rows)?))
                    }
                    _ => unreachable!("matched above"),
                }
            }
        };
        Ok(Node { shape, rows })
    }

    fn scan(&mut self, scan: &Scan) -> Result<Node, Error> {
        let source = (self.scan)(scan)?;
        let table = self
            .session
            .static_table(source.schema.clone(), Vec::new())?;
        let shape = match &scan.predicate {
            Some(predicate) => table.filter(predicate)?,
            None => table,
        };
        let rows = match (source.reread, &scan.predicate) {
            (Some(reread), predicate) => Rows::Scanned(Scanned {
                schema: source.schema,
                parts: source.parts,
                reread,
                stats: source.stats,
                predicate: predicate.clone(),
                table: shape.description(),
            }),
            (None, Some(predicate)) => Rows::Parts(source.parts).then(
                filter(shape.description(), &source.schema, predicate)?,
                false,
            )?,
            (None, None) => Rows::Parts(source.parts),
        };
        Ok(Node { shape, rows })
    }

    /// The engine's static operator over its inputs' collected rows.
    fn engine(&self, plan: &Plan, inputs: Vec<Node>) -> Result<Rows, Error> {
        let tables = inputs
            .into_iter()
            .map(|input| {
                let rows = collect(input.rows.into_parts()?)?;
                self.session
                    .computed_table(input.shape.schema().clone(), rows)
            })
            .collect::<Result<Vec<_>, _>>()?;
        let table = plan.apply(&tables.iter().collect::<Vec<_>>())?;
        Ok(Rows::Parts(pieces(table.snapshot()?)))
    }
}

/// `where predicate` over batches of `input`'s columns, failing as `table` would.
fn filter(table: String, input: &Schema, predicate: &str) -> Result<Map, Error> {
    let filter = Where::new(Expression::predicate(predicate, input)?, true);
    Ok(Arc::new(move |batch| {
        filter.filter(&batch).map_err(|e| failed(&table, e))
    }))
}

fn failed(table: &str, error: ArrowError) -> Error {
    Error::Compute {
        table: table.to_owned(),
        message: error.to_string(),
    }
}

impl Rows {
    /// These rows with `map` applied to each batch. `keeps_rows` if it keeps every row in
    /// place, so a sort below can still be cut short by a head above.
    fn then(self, map: Map, keeps_rows: bool) -> Result<Rows, Error> {
        match self {
            Rows::Sorted(mut sort) if keeps_rows => {
                sort.after.push(map);
                Ok(Rows::Sorted(sort))
            }
            rows => Ok(Rows::Parts(mapped(rows.into_parts()?, &map))),
        }
    }

    fn into_parts(self) -> Result<Vec<Part>, Error> {
        match self {
            Rows::Parts(parts) => Ok(parts),
            Rows::Scanned(scanned) => scanned.into_parts(),
            Rows::Sorted(mut sort) => {
                let input = std::mem::replace(&mut sort.input, Box::new(Rows::Parts(Vec::new())));
                let input = collect(input.into_parts()?)?;
                let rows =
                    sorted(&sort.schema, &input, &sort.keys).map_err(|e| failed(&sort.table, e))?;
                Ok(sort.then(rows))
            }
        }
    }

    /// The rows at positions `start..stop` (to the end if `None`).
    fn slice(self, start: usize, stop: Option<usize>) -> Result<Rows, Error> {
        if stop.is_some_and(|stop| stop <= start) {
            return Ok(Rows::Parts(Vec::new()));
        }
        let parts = match (self, stop) {
            (Rows::Sorted(mut sort), Some(stop)) => {
                let input = std::mem::replace(&mut sort.input, Box::new(Rows::Parts(Vec::new())));
                let keys: Vec<usize> = sort.keys.iter().map(|&(at, _)| at).collect();
                let best = match *input {
                    Rows::Scanned(scanned) => match scanned.narrow(&keys) {
                        Some(narrow) => scanned.top(&narrow, &sort.keys, stop, &sort.table)?,
                        None => {
                            let skip = scanned.skipping(&sort.keys);
                            top(
                                scanned.into_parts()?,
                                &sort.schema,
                                &sort.keys,
                                stop,
                                &sort.table,
                                skip,
                            )?
                        }
                    },
                    input => top(
                        input.into_parts()?,
                        &sort.schema,
                        &sort.keys,
                        stop,
                        &sort.table,
                        None,
                    )?,
                };
                sort.then(best)
            }
            (rows, _) => rows.into_parts()?,
        };
        Ok(Rows::Parts(pieces(head(parts, start, stop)?)))
    }
}

impl Scanned {
    /// An aggregation without keys of only `count(*)`, `min`, and `max` over the scan, which has
    /// no predicate, from what the source's statistics say of each part, reading only the parts
    /// they say nothing of, and of those only the columns asked. `None` where it does not apply:
    /// another aggregation, a column of a type whose extremes the engine orders otherwise than
    /// statistics do (floats: Parquet leaves NaN out), or no rows, which the engine answers.
    fn summarized(
        &self,
        aggregations: &[(String, String)],
        output: &SchemaRef,
    ) -> Result<Option<RecordBatch>, Error> {
        enum Wanted {
            Count,
            Min(usize),
            Max(usize),
        }
        let Some(stats) = &self.stats else {
            return Ok(None);
        };
        if self.predicate.is_some() {
            return Ok(None);
        }
        let column = |name: &str| {
            self.schema.index_of(name).ok().filter(|&at| {
                matches!(
                    self.schema.field(at).data_type(),
                    DataType::Int16
                        | DataType::Int32
                        | DataType::Int64
                        | DataType::Date32
                        | DataType::Timestamp(_, _)
                        | DataType::Utf8
                )
            })
        };
        let mut wanted = Vec::with_capacity(aggregations.len());
        let mut columns = BTreeSet::new();
        for (_, spec) in aggregations {
            let want = match (agg::function(spec).as_deref(), agg::reads(spec)) {
                (Some("count"), None) => Wanted::Count,
                (Some("min"), Some(name)) => match column(name) {
                    Some(at) => Wanted::Min(at),
                    None => return Ok(None),
                },
                (Some("max"), Some(name)) => match column(name) {
                    Some(at) => Wanted::Max(at),
                    None => return Ok(None),
                },
                _ => return Ok(None),
            };
            if let Wanted::Min(at) | Wanted::Max(at) = want {
                columns.insert(at);
            }
            wanted.push(want);
        }
        let columns: Vec<usize> = columns.into_iter().collect();
        let failed = |e| failed(&self.table, e);
        let mut rows = 0u64;
        let (mut mins, mut maxes) = (
            vec![Vec::new(); columns.len()],
            vec![Vec::new(); columns.len()],
        );
        for part in 0..self.parts.len() {
            if let Some(known) = stats(part, &columns)?
                && known.whole
                && known.columns.iter().all(Option::is_some)
            {
                rows += known.rows;
                for (at, column) in known.columns.into_iter().flatten().enumerate() {
                    mins[at].push(column.min);
                    maxes[at].push(column.max);
                }
                continue;
            }
            // Read: the columns asked, or the first alone for its rows.
            let read = if columns.is_empty() {
                vec![0]
            } else {
                columns.clone()
            };
            for batch in (self.reread)(part, &read, None)? {
                let batch = batch?;
                rows += batch.num_rows() as u64;
                for at in 0..columns.len() {
                    let (min, max) = extremes(batch.column(at)).map_err(failed)?;
                    mins[at].push(min);
                    maxes[at].push(max);
                }
            }
        }
        if rows == 0 {
            return Ok(None);
        }
        let mut out = Vec::with_capacity(wanted.len());
        for (want, field) in wanted.iter().zip(output.fields()) {
            let array: ArrayRef = match want {
                Wanted::Count => Arc::new(Int64Array::from(vec![rows as i64])),
                Wanted::Min(at) | Wanted::Max(at) => {
                    let at = columns.binary_search(at).expect("asked");
                    let max = matches!(want, Wanted::Max(_));
                    let of = if max { &maxes[at] } else { &mins[at] };
                    let all = concat(&of.iter().map(AsRef::as_ref).collect::<Vec<_>>())
                        .map_err(failed)?;
                    let (least, greatest) = extremes(&all).map_err(failed)?;
                    if max { greatest } else { least }
                }
            };
            out.push(cast(&array, field.data_type()).map_err(failed)?);
        }
        RecordBatch::try_new(output.clone(), out)
            .map(Some)
            .map_err(failed)
    }

    /// The skipping of parts a top-N by `keys` over the scan can do, where its source has
    /// statistics.
    fn skipping(&self, keys: &[(usize, bool)]) -> Option<Skip> {
        let (&(column, descending), stats) = (keys.first()?, self.stats.clone()?);
        Some(Skip {
            stats,
            column,
            descending,
        })
    }

    /// Whether a selection of the columns at `at` keeps every column the predicate reads.
    fn keeps(&self, at: &[usize]) -> bool {
        let Some(predicate) = &self.predicate else {
            return true;
        };
        reads(predicate).is_some_and(|names| {
            names
                .iter()
                .filter_map(|name| self.schema.index_of(name).ok())
                .all(|column| at.contains(&column))
        })
    }

    /// The scan's columns at `at`, as `schema`: its parts through `map`, and its reads again
    /// and statistics asked by position in the selection, answered by position in the scan.
    fn selected(self, at: Vec<usize>, schema: SchemaRef, map: Map) -> Scanned {
        let at: Arc<[usize]> = at.into();
        let reread: Reread = {
            let (at, read, table) = (at.clone(), self.reread, self.table.clone());
            Arc::new(move |part, columns, rows| {
                // The scan reads its columns in its order: ask in that order, then put back.
                let mut asked: Vec<(usize, usize)> = columns
                    .iter()
                    .enumerate()
                    .map(|(place, &column)| (at[column], place))
                    .collect();
                asked.sort_unstable();
                let mut from = vec![0; asked.len()];
                for (got, &(_, place)) in asked.iter().enumerate() {
                    from[place] = got;
                }
                let ascending: Vec<usize> = asked.iter().map(|&(column, _)| column).collect();
                let table = table.clone();
                let batches = read(part, &ascending, rows)?;
                Ok(Box::new(batches.map(move |batch| {
                    let batch = batch?;
                    let fields: Vec<_> = from
                        .iter()
                        .map(|&got| batch.schema().field(got).clone())
                        .collect();
                    let columns = from.iter().map(|&got| batch.column(got).clone()).collect();
                    let options = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
                    RecordBatch::try_new_with_options(
                        Arc::new(Schema::new(fields)),
                        columns,
                        &options,
                    )
                    .map_err(|e| failed(&table, e))
                })) as Batches)
            })
        };
        let stats = self.stats.map(|stats| {
            let at = at.clone();
            Arc::new(move |part: usize, columns: &[usize]| {
                let columns: Vec<usize> = columns.iter().map(|&column| at[column]).collect();
                stats(part, &columns)
            }) as Stats
        });
        Scanned {
            schema,
            parts: mapped(self.parts, &map),
            reread,
            stats,
            predicate: self.predicate,
            table: self.table,
        }
    }

    /// The scan's rows, through its predicate: read narrowly first where the predicate reads
    /// fewer than half the columns, since reading again costs each part a second read, which
    /// pays only where most of the columns are left for the rows kept.
    fn into_parts(self) -> Result<Vec<Part>, Error> {
        let Some(predicate) = &self.predicate else {
            return Ok(self.parts);
        };
        if let Some(narrow) = self.narrow(&[])
            && narrow.columns.len() * 2 < self.schema.fields().len()
        {
            return Ok(self.filtered(&narrow));
        }
        let filter = filter(self.table.clone(), &self.schema, predicate)?;
        Ok(mapped(self.parts, &filter))
    }

    /// A narrow read of the columns at `also` and those the predicate reads, if they are fewer
    /// than all and the predicate is known to read only those.
    fn narrow(&self, also: &[usize]) -> Option<Narrow> {
        if self.parts.len() as u64 > u64::MAX >> PART_BITS {
            return None;
        }
        let mut columns: BTreeSet<usize> = also.iter().copied().collect();
        if let Some(predicate) = &self.predicate {
            columns.extend(
                reads(predicate)?
                    .iter()
                    .filter_map(|name| self.schema.index_of(name).ok()),
            );
        }
        if columns.len() == self.schema.fields().len() {
            return None;
        }
        let columns: Arc<[usize]> = columns.into_iter().collect();
        let read = self.schema.project(&columns).ok()?;
        let filter = match &self.predicate {
            // A predicate that does not bind to these columns reads more than `reads` says:
            // the scan is then read whole.
            Some(predicate) => Some(filter(self.table.clone(), &read, predicate).ok()?),
            None => None,
        };
        let mut fields = read.fields().to_vec();
        fields.push(Arc::new(Field::new("position", DataType::UInt64, false)));
        Some(Narrow {
            columns,
            schema: Arc::new(Schema::new(fields)),
            filter,
        })
    }

    /// Each part read narrowly, each row with its position, through the predicate.
    fn narrow_parts(&self, narrow: &Narrow) -> Vec<Part> {
        (0..self.parts.len())
            .map(|index| {
                let reread = self.reread.clone();
                let (columns, schema) = (narrow.columns.clone(), narrow.schema.clone());
                let (filter, table) = (narrow.filter.clone(), self.table.clone());
                Box::new(move || {
                    let batches = reread(index, &columns, None)?;
                    let mut next = (index as u64) << PART_BITS;
                    let positioned = batches.map(move |batch| {
                        let batch = batch?;
                        let end = next + batch.num_rows() as u64;
                        if end > next && (end - 1) >> PART_BITS != index as u64 {
                            return Err(Error::Unsupported(format!(
                                "a part of a scan read again reads at most 2^{PART_BITS} rows"
                            )));
                        }
                        let mut columns = batch.columns().to_vec();
                        columns.push(Arc::new(UInt64Array::from_iter_values(next..end)));
                        next = end;
                        let batch = RecordBatch::try_new(schema.clone(), columns)
                            .map_err(|e| failed(&table, e))?;
                        match &filter {
                            Some(filter) => filter(batch),
                            None => Ok(batch),
                        }
                    });
                    Ok(Box::new(positioned) as Batches)
                }) as Part
            })
            .collect()
    }

    /// `top` over the scan, choosing rows on the narrow columns alone, then reading every
    /// column of the winners only.
    fn top(
        &self,
        narrow: &Narrow,
        keys: &[(usize, bool)],
        stop: usize,
        table: &str,
    ) -> Result<Vec<RecordBatch>, Error> {
        let skip = self.skipping(keys);
        let keys: Vec<(usize, bool)> = keys
            .iter()
            .map(|&(at, descending)| {
                let at = narrow.columns.binary_search(&at).expect("keys are read");
                (at, descending)
            })
            .collect();
        let winners = top(
            self.narrow_parts(narrow),
            &narrow.schema,
            &keys,
            stop,
            table,
            skip,
        )?;
        let positions: Vec<u64> = winners
            .iter()
            .flat_map(|batch| positions_of(batch).values().iter().copied())
            .collect();
        self.fetch(&positions)
    }

    /// Every column of the rows at `positions`, in that order: of each part holding any, those
    /// rows alone, a group of columns to each thread, since decoding a page of each of many
    /// columns is most of reading a few rows.
    fn fetch(&self, positions: &[u64]) -> Result<Vec<RecordBatch>, Error> {
        if positions.is_empty() {
            return Ok(Vec::new());
        }
        let mut wanted = positions.to_vec();
        wanted.sort_unstable();
        let width = self.schema.fields().len();
        let groups: Vec<Arc<[usize]>> = offsets(width, width.div_ceil(threads().max(1)))
            .map(|(start, len)| (start..start + len).collect())
            .collect();
        let runs: Vec<(usize, Arc<[u64]>)> = wanted
            .chunk_by(|a, b| a >> PART_BITS == b >> PART_BITS)
            .map(|run| {
                let rows = run.iter().map(|position| position & ROW).collect();
                ((run[0] >> PART_BITS) as usize, rows)
            })
            .collect();
        let mut reads = Vec::with_capacity(groups.len() * runs.len());
        for group in &groups {
            for (part, rows) in &runs {
                let (reread, group, part, rows) =
                    (self.reread.clone(), group.clone(), *part, rows.clone());
                reads.push(Box::new(move || reread(part, &group, Some(&rows))) as Part);
            }
        }
        let mut read = Vec::with_capacity(reads.len());
        drive(
            reads,
            false,
            |_, batches| batches.collect::<Result<Vec<_>, _>>(),
            |batches| {
                read.push(batches);
                Ok(true)
            },
        )?;
        let mut columns = Vec::with_capacity(width);
        for (group, read) in groups.iter().zip(read.chunks(runs.len().max(1))) {
            let schema = self
                .schema
                .project(group)
                .map_err(|e| failed(&self.table, e))?;
            let read = concat_batches(&Arc::new(schema), read.iter().flatten())
                .map_err(|e| failed(&self.table, e))?;
            all_read(&self.table, read.num_rows(), wanted.len())?;
            columns.extend(read.columns().iter().cloned());
        }
        let options = RecordBatchOptions::new().with_row_count(Some(wanted.len()));
        let read = RecordBatch::try_new_with_options(self.schema.clone(), columns, &options)
            .map_err(|e| failed(&self.table, e))?;
        let order: UInt64Array = positions
            .iter()
            .map(|position| wanted.binary_search(position).expect("asked") as u64)
            .collect();
        let rows = take_record_batch(&read, &order).map_err(|e| failed(&self.table, e))?;
        Ok(vec![rows])
    }

    /// Each part through the predicate, reading the predicate's columns for every row, then
    /// the others for the rows it keeps alone.
    fn filtered(&self, narrow: &Narrow) -> Vec<Part> {
        let rest: Arc<[usize]> = (0..self.schema.fields().len())
            .filter(|at| narrow.columns.binary_search(at).is_err())
            .collect();
        let rest_schema = Arc::new(
            self.schema
                .project(&rest)
                .expect("the schema's own columns"),
        );
        self.narrow_parts(narrow)
            .into_iter()
            .enumerate()
            .map(|(index, part)| {
                let schema = self.schema.clone();
                let (read, rest, rest_schema) =
                    (narrow.columns.clone(), rest.clone(), rest_schema.clone());
                let (reread, table) = (self.reread.clone(), self.table.clone());
                Box::new(move || {
                    let kept = part()?
                        .filter(|batch| !matches!(batch, Ok(batch) if batch.num_rows() == 0))
                        .collect::<Result<Vec<_>, _>>()?;
                    let rows: Vec<u64> = kept
                        .iter()
                        .flat_map(|batch| positions_of(batch).values().iter())
                        .map(|position| position & ROW)
                        .collect();
                    if rows.is_empty() {
                        return Ok(Box::new(std::iter::empty()) as Batches);
                    }
                    let others =
                        reread(index, &rest, Some(&rows))?.collect::<Result<Vec<_>, _>>()?;
                    let others =
                        concat_batches(&rest_schema, &others).map_err(|e| failed(&table, e))?;
                    all_read(&table, others.num_rows(), rows.len())?;
                    // The kept rows as the predicate's batches left them, beside the others'.
                    let mut offset = 0;
                    let mut batches = Vec::with_capacity(kept.len());
                    for kept in kept {
                        let others = others.slice(offset, kept.num_rows());
                        offset += kept.num_rows();
                        let columns = (0..schema.fields().len())
                            .map(|at| match read.binary_search(&at) {
                                Ok(at) => kept.column(at).clone(),
                                Err(_) => {
                                    let at = rest.binary_search(&at).expect("the rest");
                                    others.column(at).clone()
                                }
                            })
                            .collect();
                        let batch = RecordBatch::try_new(schema.clone(), columns)
                            .map_err(|e| failed(&table, e));
                        batches.push(batch);
                    }
                    Ok(Box::new(batches.into_iter()) as Batches)
                }) as Part
            })
            .collect()
    }
}

/// The positions of a narrow read's rows: its last column.
fn positions_of(batch: &RecordBatch) -> &UInt64Array {
    batch.columns()[batch.num_columns() - 1].as_primitive::<UInt64Type>()
}

/// That a source read again gave as many rows as were asked of it.
fn all_read(table: &str, read: usize, asked: usize) -> Result<(), Error> {
    if read == asked {
        return Ok(());
    }
    Err(Error::Source {
        table: table.to_owned(),
        message: format!("read again, it gave {read} rows of the {asked} asked"),
    })
}

impl Sort {
    /// `rows`, sorted, through the operators after the sort.
    fn then(&self, rows: Vec<RecordBatch>) -> Vec<Part> {
        self.after
            .iter()
            .fold(pieces(rows), |parts, map| mapped(parts, map))
    }
}

/// Each part's batches through `map`, as they are read.
fn mapped(parts: Vec<Part>, map: &Map) -> Vec<Part> {
    parts
        .into_iter()
        .map(|part| {
            let map = map.clone();
            Box::new(move || {
                let batches = part()?;
                Ok(
                    Box::new(batches.map(move |batch| batch.and_then(|batch| map(batch))))
                        as Batches,
                )
            }) as Part
        })
        .collect()
}

/// Rows already computed, as parts of at most `PIECE_ROWS` rows, so the operators over them
/// still spread over threads.
fn pieces(batches: Vec<RecordBatch>) -> Vec<Part> {
    batches
        .iter()
        .flat_map(|batch| {
            offsets(batch.num_rows(), PIECE_ROWS).map(move |(offset, len)| batch.slice(offset, len))
        })
        .map(|piece| -> Part { Box::new(move || Ok(Box::new(std::iter::once(Ok(piece))))) })
        .collect()
}

/// Every row, in order, without empty batches.
fn collect(parts: Vec<Part>) -> Result<Vec<RecordBatch>, Error> {
    let mut rows = Vec::new();
    drive(
        parts,
        false,
        |_, batches| {
            batches
                .filter(|batch| !matches!(batch, Ok(batch) if batch.num_rows() == 0))
                .collect::<Result<Vec<_>, _>>()
        },
        |part| {
            rows.extend(part);
            Ok(true)
        },
    )?;
    Ok(rows)
}

/// The rows at positions `start..stop` (to the end if `None`). With a stop, each part reads
/// only up to `stop` rows, and parts are opened only while rows are still wanted.
fn head(parts: Vec<Part>, start: usize, stop: Option<usize>) -> Result<Vec<RecordBatch>, Error> {
    let Some(stop) = stop else {
        return Ok(skip(collect(parts)?, start));
    };
    let (mut rows, mut held) = (Vec::new(), 0);
    drive(
        parts,
        true,
        |_, mut batches| {
            let (mut rows, mut held) = (Vec::new(), 0);
            while held < stop {
                let Some(batch) = batches.next() else {
                    break;
                };
                let batch = batch?;
                let take = batch.num_rows().min(stop - held);
                if take > 0 {
                    rows.push(batch.slice(0, take));
                    held += take;
                }
            }
            Ok(rows)
        },
        |part| {
            for batch in part {
                let take = batch.num_rows().min(stop - held);
                if take == 0 {
                    break;
                }
                rows.push(batch.slice(0, take));
                held += take;
            }
            Ok(held < stop)
        },
    )?;
    Ok(skip(rows, start))
}

/// The last `n` rows. Each part keeps only its last `n`, and so do the parts so far.
fn tail(parts: Vec<Part>, n: usize) -> Result<Vec<RecordBatch>, Error> {
    if n == 0 {
        return Ok(Vec::new());
    }
    let mut rows = Vec::new();
    drive(
        parts,
        false,
        |_, batches| {
            let mut rows = Vec::new();
            for batch in batches {
                rows.push(batch?);
                last_batches(&mut rows, n);
            }
            Ok(rows)
        },
        |part| {
            rows.extend(part);
            last_batches(&mut rows, n);
            Ok(true)
        },
    )?;
    let count = count(&rows);
    Ok(skip(rows, count.saturating_sub(n)))
}

/// Drops the leading batches that hold none of the last `n` rows.
fn last_batches(batches: &mut Vec<RecordBatch>, n: usize) {
    let mut rows = count(batches);
    let unneeded = batches
        .iter()
        .take_while(|batch| {
            let after = rows - batch.num_rows();
            let unneeded = after >= n;
            if unneeded {
                rows = after;
            }
            unneeded
        })
        .count();
    batches.drain(..unneeded);
}

fn count(batches: &[RecordBatch]) -> usize {
    batches.iter().map(RecordBatch::num_rows).sum()
}

/// `batches` without their first `start` rows.
fn skip(batches: Vec<RecordBatch>, mut start: usize) -> Vec<RecordBatch> {
    batches
        .into_iter()
        .filter_map(|batch| {
            let rows = batch.num_rows();
            if start >= rows {
                start -= rows;
                return None;
            }
            let from = std::mem::take(&mut start);
            Some(batch.slice(from, rows - from))
        })
        .collect()
}

/// A column's least and greatest non-NULL values, as one-row arrays (NULL where it has none).
fn extremes(array: &ArrayRef) -> Result<(ArrayRef, ArrayRef), ArrowError> {
    let first = |descending| -> Result<ArrayRef, ArrowError> {
        let options = SortOptions {
            descending,
            nulls_first: false,
        };
        let at = sort_to_indices(array, Some(options), Some(1))?;
        if at.is_empty() {
            return Ok(new_null_array(array.data_type(), 1));
        }
        take(array, &at, None)
    };
    Ok((first(false)?, first(true)?))
}

/// Which parts of a top-N's scan to leave unread: those whose statistics show every row worse
/// on the first sort key than the `stop`th best row so far, which then beats them all.
struct Skip {
    stats: Stats,
    /// The first sort key's position in the scan.
    column: usize,
    descending: bool,
}

impl Skip {
    /// Whether part `part` holds no row better than `cutoff`, the `stop`th best row's first key.
    /// Equal keys are kept: later keys, or input order, may still put them ahead. NULL sorts as
    /// the largest value: last ascending, so never ahead of a value; first descending.
    fn skips(&self, part: usize, cutoff: &ArrayRef) -> Result<bool, Error> {
        if cutoff.is_null(0) {
            return Ok(false);
        }
        let Some(known) = (self.stats)(part, &[self.column])? else {
            return Ok(false);
        };
        let Some(Some(column)) = known.columns.first() else {
            return Ok(false);
        };
        let worse = if self.descending {
            if column.nulls > 0 || column.max.is_null(0) {
                return Ok(false);
            }
            cmp::lt(&column.max, cutoff)
        } else {
            if column.min.is_null(0) {
                return Ok(true);
            }
            cmp::gt(&column.min, cutoff)
        };
        // Statistics of another type than the key's skip nothing.
        Ok(worse.is_ok_and(|worse| worse.value(0)))
    }
}

/// The first `stop` rows of the input sorted by `keys`, ties in input order (a top-N): each
/// part keeps its best `stop` rows, and parts merge in their order, so ties stay in input
/// order. Choosing reads only the key columns; whole rows are gathered for the winners alone.
/// With `skip`, once `stop` winners are known, a part that cannot beat the last is not read.
fn top(
    parts: Vec<Part>,
    schema: &SchemaRef,
    keys: &[(usize, bool)],
    stop: usize,
    table: &str,
    skip: Option<Skip>,
) -> Result<Vec<RecordBatch>, Error> {
    // Earlier rows first: the winners so far, then the rows read since.
    let best = |rows: Vec<RecordBatch>| -> Result<Vec<RecordBatch>, Error> {
        let order = order(schema, &rows, keys).map_err(|e| failed(table, e))?;
        gather(&rows, &order[..stop.min(order.len())]).map_err(|e| failed(table, e))
    };
    // Rows are chosen once at least as many as are kept arrive, so each row takes part in a
    // bounded number of sorts however large `stop` is.
    let enough = stop.saturating_add(stop.max(PIECE_ROWS));
    // The `stop`th best row's first key, once `stop` rows are known: what a part must beat.
    let cutoff: Arc<Mutex<Option<ArrayRef>>> = Arc::default();
    // Checked as a part opens, before it reads anything.
    let skipping = skip.is_some();
    let parts = match skip.map(Arc::new) {
        None => parts,
        Some(skip) => parts
            .into_iter()
            .enumerate()
            .map(|(index, part)| {
                let (skip, cutoff) = (skip.clone(), cutoff.clone());
                Box::new(move || {
                    let cut = cutoff
                        .lock()
                        .unwrap_or_else(PoisonError::into_inner)
                        .clone();
                    if let Some(cut) = cut
                        && skip.skips(index, &cut)?
                    {
                        return Ok(Box::new(std::iter::empty()) as Batches);
                    }
                    part()
                }) as Part
            })
            .collect(),
    };
    let mut winners = Vec::new();
    drive(
        parts,
        false,
        |_, batches| {
            let mut rows = Vec::new();
            for batch in batches {
                rows.push(batch?);
                if count(&rows) >= enough {
                    rows = best(rows)?;
                }
            }
            // Fewer rows than are kept go to the merge as read, which sorts them anyway.
            if count(&rows) <= stop {
                return Ok(rows);
            }
            best(rows)
        },
        |part| {
            winners.extend(part);
            winners = best(std::mem::take(&mut winners))?;
            if skipping && count(&winners) >= stop {
                let last = row_of(&winners, stop - 1)
                    .map(|(batch, row)| batch.column(keys[0].0).slice(row, 1));
                *cutoff.lock().unwrap_or_else(PoisonError::into_inner) = last;
            }
            Ok(true)
        },
    )?;
    Ok(winners)
}

/// The batch holding row `row` of `batches`, and its place in it.
fn row_of(batches: &[RecordBatch], mut row: usize) -> Option<(&RecordBatch, usize)> {
    for batch in batches {
        if row < batch.num_rows() {
            return Some((batch, row));
        }
        row -= batch.num_rows();
    }
    None
}

/// `agg_by` over the parts: each folds into partial states on the thread that reads it, and
/// each sealed part goes, in part order, to every shard's merger, which merges the groups of one
/// range of key hashes on a thread of its own while later parts fold.
fn aggregate(
    parts: Vec<Part>,
    fold: &Fold,
    output: &SchemaRef,
    table: &str,
) -> Result<Vec<RecordBatch>, Error> {
    let failed = |e| failed(table, e);
    if parts.len() >= 1 << (RowKey::BITS - PART_BITS) {
        return Err(Error::Unsupported(format!(
            "an aggregation reads at most {} parts",
            1u64 << (RowKey::BITS - PART_BITS)
        )));
    }
    let shards = threads();
    let mut merged = fold.merged(shards).map_err(failed)?;
    merged.merge_from(
        |give| {
            drive(
                parts,
                false,
                |index, batches| {
                    let mut part = fold.fresh().map_err(failed)?;
                    let mut rows: RowKey = 0;
                    for batch in batches {
                        let batch = batch?;
                        let key = ((index as RowKey) << PART_BITS) | rows;
                        part.add(&batch, key).map_err(failed)?;
                        rows += batch.num_rows() as RowKey;
                        if rows >> PART_BITS != 0 {
                            return Err(Error::Unsupported(format!(
                                "an aggregation reads at most 2^{PART_BITS} rows of one part"
                            )));
                        }
                    }
                    part.seal(shards).map_err(failed)?;
                    Ok(part)
                },
                // A shard that failed stops the reading; its error comes back from the merge.
                |part| Ok(give(part)),
            )
        },
        failed,
    )?;
    merged.finish(output).map_err(failed)
}

/// What the threads share while running parts.
struct Queue<T> {
    parts: Vec<Option<Part>>,
    /// Each part's result, until taken.
    done: Vec<Option<thread::Result<Result<T, Error>>>>,
    /// The next part to open.
    next: usize,
    /// Parts whose results were taken.
    taken: usize,
    /// How many parts may be open or waiting to be taken.
    ahead: usize,
    stop: bool,
}

fn lock<T>(queue: &Mutex<Queue<T>>) -> MutexGuard<'_, Queue<T>> {
    queue.lock().unwrap_or_else(PoisonError::into_inner)
}

/// Runs `work` on each part, opened on whichever thread runs it, and hands each result to `take`
/// on the calling thread in part order, until `take` says it needs no more: later parts are then
/// not opened. Only a few parts per thread run ahead of the one `take` is owed, so memory
/// follows a few parts' results however many there are. A `growing` run opens one part first
/// and doubles how many run ahead each time `take` wants more, so a head opens only the parts
/// it needs.
fn drive<T: Send>(
    parts: Vec<Part>,
    growing: bool,
    work: impl Fn(usize, Batches) -> Result<T, Error> + Sync,
    mut take: impl FnMut(T) -> Result<bool, Error>,
) -> Result<(), Error> {
    let count = parts.len();
    let threads = threads().min(count);
    if threads <= 1 {
        for (index, part) in parts.into_iter().enumerate() {
            if !take(work(index, part()?)?)? {
                break;
            }
        }
        return Ok(());
    }
    let most = 2 * threads;
    let queue = Mutex::new(Queue {
        parts: parts.into_iter().map(Some).collect(),
        done: (0..count).map(|_| None).collect(),
        next: 0,
        taken: 0,
        ahead: if growing { 1 } else { most },
        stop: false,
    });
    let changed = Condvar::new();
    let (queue, changed, work) = (&queue, &changed, &work);
    thread::scope(|scope| {
        for _ in 0..threads {
            scope.spawn(move || {
                while let Some((index, part)) = next_part(queue, changed) {
                    let result = panic::catch_unwind(AssertUnwindSafe(|| {
                        part().and_then(|batches| work(index, batches))
                    }));
                    lock(queue).done[index] = Some(result);
                    changed.notify_all();
                }
            });
        }
        // However this thread leaves, the workers stop opening parts and finish.
        let _stop = Stop { queue, changed };
        loop {
            let result = {
                let mut state = lock(queue);
                loop {
                    if state.taken == count {
                        return Ok(());
                    }
                    let at = state.taken;
                    if let Some(result) = state.done[at].take() {
                        break result;
                    }
                    state = changed.wait(state).unwrap_or_else(PoisonError::into_inner);
                }
            };
            let result = result.unwrap_or_else(|payload| panic::resume_unwind(payload));
            if !take(result?)? {
                return Ok(());
            }
            let mut state = lock(queue);
            state.taken += 1;
            if growing {
                state.ahead = (state.ahead * 2).min(most);
            }
            drop(state);
            changed.notify_all();
        }
    })
}

/// The next part a worker may open, or `None` once none is left or wanted.
fn next_part<T>(queue: &Mutex<Queue<T>>, changed: &Condvar) -> Option<(usize, Part)> {
    let mut state = lock(queue);
    loop {
        if state.stop || state.next == state.parts.len() {
            return None;
        }
        if state.next < state.taken + state.ahead {
            break;
        }
        state = changed.wait(state).unwrap_or_else(PoisonError::into_inner);
    }
    let index = state.next;
    state.next += 1;
    let part = state.parts[index].take().expect("each part runs once");
    Some((index, part))
}

struct Stop<'a, T> {
    queue: &'a Mutex<Queue<T>>,
    changed: &'a Condvar,
}

impl<T> Drop for Stop<'_, T> {
    fn drop(&mut self) {
        lock(self.queue).stop = true;
        self.changed.notify_all();
    }
}
