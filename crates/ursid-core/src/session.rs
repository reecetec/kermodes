//! A session owns one update graph: input tables, the tables derived from them, and cycles.

use std::sync::{Arc, Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};

use arrow::array::{ArrayRef, AsArray};
use arrow::compute::filter_record_batch;
use arrow::datatypes::{Field, FieldRef, Schema, SchemaRef};
use arrow::error::ArrowError;
use arrow::record_batch::{RecordBatch, RecordBatchOptions};

use crate::{Error, Expression, type_name};

/// An engine instance: one update graph and the tables in it. Clones share the graph.
#[derive(Clone, Default)]
pub struct Session {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    graph: RwLock<Graph>,
    /// Appends waiting for the next cycle, in arrival order.
    pending: Mutex<Vec<(usize, RecordBatch)>>,
}

#[derive(Default)]
struct Graph {
    /// In creation order, which is topological: a table is built only from existing tables.
    tables: Vec<TableState>,
    cycle: u64,
}

struct TableState {
    description: String,
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
    refreshing: bool,
    failure: Option<Error>,
    operator: Operator,
}

enum Operator {
    Input,
    Static,
    Derived {
        parent: usize,
        /// Parent batches already processed.
        consumed: usize,
        op: Derived,
    },
}

enum Derived {
    Where(Expression),
    Update(Vec<Assignment>),
}

struct Assignment {
    /// The schema the expression is bound to: the parent's plus earlier assignments.
    input: SchemaRef,
    expression: Expression,
    /// The column it writes: an existing index to replace, or the next index to append.
    position: usize,
}

/// A table in a session. Cheap to clone; clones refer to the same table.
#[derive(Clone)]
pub struct Table {
    session: Session,
    id: usize,
    schema: SchemaRef,
}

impl Session {
    pub fn new() -> Self {
        Self::default()
    }

    /// A refreshing table that receives rows through `Table::append`. Every column is nullable.
    pub fn input_table(&self, schema: &Schema) -> Result<Table, Error> {
        let fields: Vec<Field> = schema
            .fields()
            .iter()
            .map(|f| f.as_ref().clone().with_nullable(true))
            .collect();
        for (i, field) in fields.iter().enumerate() {
            if fields[..i].iter().any(|f| f.name() == field.name()) {
                return Err(Error::Schema(format!(
                    "duplicate column `{}`",
                    field.name()
                )));
            }
        }
        Ok(self.add(TableState {
            description: "input".into(),
            schema: Arc::new(Schema::new(fields)),
            batches: Vec::new(),
            refreshing: true,
            failure: None,
            operator: Operator::Input,
        }))
    }

    /// A table that never changes. Tables built from it are computed once, immediately.
    pub fn static_table(
        &self,
        schema: SchemaRef,
        batches: Vec<RecordBatch>,
    ) -> Result<Table, Error> {
        let batches = conform_all(&schema, &batches)?;
        Ok(self.add(TableState {
            description: "static".into(),
            schema,
            batches,
            refreshing: false,
            failure: None,
            operator: Operator::Static,
        }))
    }

    /// Runs one cycle: applies pending appends, then updates every refreshing derived table in
    /// creation order. Returns the cycle number.
    pub fn step(&self) -> u64 {
        let pending = std::mem::take(&mut *self.pending());
        let mut graph = self.write();
        for (id, batch) in pending {
            graph.tables[id].batches.push(batch);
        }
        for index in 0..graph.tables.len() {
            let Operator::Derived { parent, .. } = graph.tables[index].operator else {
                continue;
            };
            if !graph.tables[index].refreshing {
                continue;
            }
            let (before, rest) = graph.tables.split_at_mut(index);
            advance(&before[parent], &mut rest[0]);
        }
        graph.cycle += 1;
        graph.cycle
    }

    fn add(&self, state: TableState) -> Table {
        let schema = state.schema.clone();
        let mut graph = self.write();
        graph.tables.push(state);
        Table {
            session: self.clone(),
            id: graph.tables.len() - 1,
            schema,
        }
    }

    fn read(&self) -> RwLockReadGuard<'_, Graph> {
        self.inner
            .graph
            .read()
            .expect("ursid graph lock poisoned by an earlier panic")
    }

    fn write(&self) -> RwLockWriteGuard<'_, Graph> {
        self.inner
            .graph
            .write()
            .expect("ursid graph lock poisoned by an earlier panic")
    }

    fn pending(&self) -> std::sync::MutexGuard<'_, Vec<(usize, RecordBatch)>> {
        self.inner
            .pending
            .lock()
            .expect("ursid append queue poisoned by an earlier panic")
    }
}

impl Table {
    pub fn schema(&self) -> &SchemaRef {
        &self.schema
    }

    pub fn description(&self) -> String {
        self.session.read().tables[self.id].description.clone()
    }

    pub fn is_refreshing(&self) -> bool {
        self.session.read().tables[self.id].refreshing
    }

    /// Queues rows for the next cycle. Columns are matched by name; all batches are checked
    /// before any is queued.
    pub fn append(&self, batches: &[RecordBatch]) -> Result<(), Error> {
        if !matches!(
            self.session.read().tables[self.id].operator,
            Operator::Input
        ) {
            return Err(Error::Unsupported(format!(
                "only input tables accept appends; `{}` is derived",
                self.description()
            )));
        }
        let batches = conform_all(&self.schema, batches)?;
        self.session
            .pending()
            .extend(batches.into_iter().map(|b| (self.id, b)));
        Ok(())
    }

    /// Rows for which `predicate` is true. NULL counts as false.
    pub fn filter(&self, predicate: &str) -> Result<Table, Error> {
        let expression = Expression::predicate(predicate, &self.schema)?;
        self.derive(
            format!("where({predicate})"),
            self.schema.clone(),
            Derived::Where(expression),
        )
    }

    /// Adds or replaces columns, each `name = expression`. Later assignments see earlier ones.
    pub fn update(&self, assignments: &[&str]) -> Result<Table, Error> {
        if assignments.is_empty() {
            return Err(Error::Unsupported(
                "update needs at least one `name = expression`".into(),
            ));
        }
        let mut schema = self.schema.clone();
        let mut bound = Vec::with_capacity(assignments.len());
        for text in assignments {
            let (name, expression) = Expression::assignment(text, &schema)?;
            let field: FieldRef = Arc::new(Field::new(&name, expression.data_type().clone(), true));
            let mut fields: Vec<FieldRef> = schema.fields().iter().cloned().collect();
            let position = match schema.index_of(&name) {
                Ok(index) => {
                    fields[index] = field;
                    index
                }
                Err(_) => {
                    fields.push(field);
                    fields.len() - 1
                }
            };
            bound.push(Assignment {
                input: schema.clone(),
                expression,
                position,
            });
            schema = Arc::new(Schema::new(fields));
        }
        self.derive(
            format!("update({})", assignments.join(", ")),
            schema,
            Derived::Update(bound),
        )
    }

    /// The table's current rows, or the error that failed it.
    pub fn snapshot(&self) -> Result<Vec<RecordBatch>, Error> {
        let graph = self.session.read();
        let table = &graph.tables[self.id];
        match &table.failure {
            Some(error) => Err(error.clone()),
            None => Ok(table.batches.clone()),
        }
    }

    pub fn num_rows(&self) -> Result<usize, Error> {
        Ok(self.snapshot()?.iter().map(RecordBatch::num_rows).sum())
    }

    /// Builds a table from this one and computes it from this table's current rows. Building on
    /// a static table finishes here; building on a refreshing one also updates it every cycle.
    fn derive(&self, description: String, schema: SchemaRef, op: Derived) -> Result<Table, Error> {
        let mut graph = self.session.write();
        let parent = &graph.tables[self.id];
        let mut state = TableState {
            description,
            schema: schema.clone(),
            batches: Vec::new(),
            refreshing: parent.refreshing,
            failure: None,
            operator: Operator::Derived {
                parent: self.id,
                consumed: 0,
                op,
            },
        };
        advance(parent, &mut state);
        if let Some(error) = state.failure {
            return Err(error);
        }
        graph.tables.push(state);
        Ok(Table {
            session: self.session.clone(),
            id: graph.tables.len() - 1,
            schema,
        })
    }
}

/// Processes the parent batches `child` has not seen yet. A failure is kept on the table: it
/// stops updating, and every table built from it fails too.
fn advance(parent: &TableState, child: &mut TableState) {
    let Operator::Derived { consumed, op, .. } = &mut child.operator else {
        return;
    };
    if child.failure.is_some() {
        return;
    }
    if let Some(cause) = &parent.failure {
        child.failure = Some(Error::Upstream {
            table: child.description.clone(),
            cause: Arc::new(cause.clone()),
        });
        return;
    }
    for batch in &parent.batches[*consumed..] {
        match op.apply(&child.schema, batch) {
            Ok(out) if out.num_rows() > 0 => child.batches.push(out),
            Ok(_) => {}
            Err(error) => {
                child.failure = Some(Error::Compute {
                    table: child.description.clone(),
                    message: error.to_string(),
                });
                return;
            }
        }
    }
    *consumed = parent.batches.len();
}

impl Derived {
    fn apply(&self, output: &SchemaRef, batch: &RecordBatch) -> Result<RecordBatch, ArrowError> {
        match self {
            Derived::Where(predicate) => {
                let mask = predicate.evaluate(batch)?;
                filter_record_batch(batch, mask.as_boolean())
            }
            Derived::Update(assignments) => {
                let rows = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
                let mut columns: Vec<ArrayRef> = batch.columns().to_vec();
                for assignment in assignments {
                    let input = RecordBatch::try_new_with_options(
                        assignment.input.clone(),
                        columns.clone(),
                        &rows,
                    )?;
                    let value = assignment.expression.evaluate(&input)?;
                    if assignment.position == columns.len() {
                        columns.push(value);
                    } else {
                        columns[assignment.position] = value;
                    }
                }
                RecordBatch::try_new_with_options(output.clone(), columns, &rows)
            }
        }
    }
}

/// Matches each batch's columns to `schema` by name and type; empty batches are dropped.
fn conform_all(schema: &SchemaRef, batches: &[RecordBatch]) -> Result<Vec<RecordBatch>, Error> {
    batches
        .iter()
        .filter(|b| b.num_rows() > 0)
        .map(|b| conform(schema, b))
        .collect()
}

fn conform(schema: &SchemaRef, batch: &RecordBatch) -> Result<RecordBatch, Error> {
    let given = batch.schema();
    let mut columns = Vec::with_capacity(schema.fields().len());
    for field in schema.fields() {
        let index = given.index_of(field.name()).map_err(|_| {
            Error::Schema(format!(
                "missing column `{}` ({})",
                field.name(),
                type_name(field.data_type())
            ))
        })?;
        let actual = given.field(index).data_type();
        if actual != field.data_type() {
            return Err(Error::Schema(format!(
                "column `{}` is {}, but the table expects {}",
                field.name(),
                type_name(actual),
                type_name(field.data_type())
            )));
        }
        columns.push(batch.column(index).clone());
    }
    let extra: Vec<&str> = given
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .filter(|name| schema.index_of(name).is_err())
        .collect();
    if !extra.is_empty() {
        return Err(Error::Schema(format!(
            "unexpected columns: {}",
            extra.join(", ")
        )));
    }
    let rows = RecordBatchOptions::new().with_row_count(Some(batch.num_rows()));
    RecordBatch::try_new_with_options(schema.clone(), columns, &rows)
        .map_err(|e| Error::Schema(e.to_string()))
}
