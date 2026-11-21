//! `Session.replay`: stored tables' past dates fed back through a graph in event-time order, so
//! a deployment's code runs as a backtest (architecture.md §9.4, `kermodes_core::replay`).

use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use kermodes_core::replay::{Chunks, Kind};
use kermodes_weir::{Date, TableName};
use pyo3::prelude::*;
use pyo3::types::PyDict;

use crate::{KermodesError, Table, Tables, column_names, engine_error, schema_from, store_error};

/// Cycles run between checks for Ctrl-C.
const CYCLES_PER_CHECK: u64 = 10_000;

/// A replay of a store's past dates (`Session.replay`): the tables `live_table` gives tick as
/// the replay reaches each row's time, cycled by `step` or `run`. It reads the store and never
/// writes it, so a deployment run on it is a backtest.
#[pyclass(module = "kermodes", name = "Replay", frozen)]
pub struct Replay {
    replay: Mutex<kermodes_core::replay::Replay>,
    tables: Tables,
    lake: Arc<kermodes_lake::Lake>,
    first: Date,
    last: Date,
}

impl Replay {
    pub(crate) fn new(
        tables: Tables,
        lake: Arc<kermodes_lake::Lake>,
        first: Date,
        last: Date,
        step: Duration,
    ) -> PyResult<Self> {
        Ok(Self {
            replay: Mutex::new(kermodes_core::replay::Replay::new(step).map_err(engine_error)?),
            tables,
            lake,
            first,
            last,
        })
    }

    fn lock(&self) -> MutexGuard<'_, kermodes_core::replay::Replay> {
        self.replay
            .lock()
            .expect("kermodes replay poisoned by an earlier panic")
    }
}

#[pymethods]
impl Replay {
    /// The stored table `name` as its past dates replay: its rows arrive in order of `ts`, a
    /// timestamp column, each when the replay reaches its time. A keyed table replays its log
    /// of changes, starting from the first date's state. As `live_table` is on a session, so
    /// the same code builds the same graph on it.
    #[pyo3(signature = (name, ts="ts"))]
    fn live_table(&self, py: Python<'_>, name: &str, ts: &str) -> PyResult<Table> {
        let name = TableName::parse(name).map_err(store_error)?;
        let entry = py
            .detach(|| self.tables.entry(&name))
            .map_err(store_error)?;
        let schema = Arc::new(kermodes_lake::history_columns(&entry).map_err(store_error)?);
        let (tables, lake, description) = (
            self.tables.clone(),
            self.lake.clone(),
            format!("replay {name}"),
        );
        let failed = description.clone();
        let (first, last) = (self.first.days(), self.last.days());
        let chunks: Chunks = Box::new((first..=last).map(move |days| {
            let date = Date::from_days(days);
            let history = kermodes_lake::read_history(
                tables.weir(),
                &lake,
                &name,
                Some(date),
                Some(date),
                &[],
                None,
            )
            .map_err(|e| kermodes_core::Error::Compute {
                table: failed.clone(),
                message: format!("cannot read {date}: {e}"),
            })?;
            // History dates its rows; the replayed table, as a live one, does not.
            let columns: Vec<usize> = (0..history.schema.fields().len())
                .filter(|at| history.schema.field(*at).name() != kermodes_lake::DATE)
                .collect();
            history
                .batches
                .iter()
                .map(|batch| batch.project(&columns))
                .collect::<Result<Vec<_>, _>>()
                .map_err(|e| kermodes_core::Error::Compute {
                    table: failed.clone(),
                    message: e.to_string(),
                })
        }));
        let key: Vec<&str> = entry.key.iter().map(String::as_str).collect();
        let kind = if key.is_empty() {
            Kind::Append
        } else {
            Kind::Keyed {
                key: &key,
                op: kermodes_lake::OP,
            }
        };
        self.lock()
            .add(&description, &schema, ts, kind, chunks)
            .map(Table)
            .map_err(engine_error)
    }

    /// Checks that the stored table `name` exists with these columns (and key), as the code
    /// replayed declares it; a replay creates nothing.
    #[pyo3(signature = (name, columns, key=None, sort=None))]
    fn create_table(
        &self,
        py: Python<'_>,
        name: &str,
        columns: &Bound<'_, PyDict>,
        key: Option<&Bound<'_, PyAny>>,
        sort: Option<&Bound<'_, PyAny>>,
    ) -> PyResult<()> {
        let _ = sort;
        let name = TableName::parse_user(name).map_err(store_error)?;
        let schema = schema_from(columns)?;
        let key = column_names(key)?;
        let entry = py.detach(|| self.tables.entry(&name)).map_err(|e| {
            KermodesError::new_err(format!("a replay reads only stored tables: {e}"))
        })?;
        let fields = |schema: &arrow::datatypes::Schema| -> Vec<(String, String)> {
            schema
                .fields()
                .iter()
                .map(|f| (f.name().clone(), kermodes_core::type_name(f.data_type())))
                .collect()
        };
        if fields(&entry.schema) != fields(&schema) || entry.key != key {
            return Err(KermodesError::new_err(format!(
                "{name} is stored with other columns or key than this code declares: a replay \
                 reads the table as it is"
            )));
        }
        Ok(())
    }

    /// A replay never writes the store: a backtest publishes nothing.
    #[pyo3(signature = (name, table, key))]
    fn publish(&self, name: &str, table: &Table, key: &Bound<'_, PyAny>) -> PyResult<()> {
        let _ = (table, key);
        Err(KermodesError::new_err(format!(
            "a replay does not publish {name}: a backtest writes nothing to the store"
        )))
    }

    /// Runs one cycle: feeds the rows due by the next step of event time that has any, then
    /// runs the graph. That time, in nanoseconds since the epoch, or None once every row has
    /// been fed.
    fn step(&self, py: Python<'_>) -> PyResult<Option<i64>> {
        py.detach(|| self.lock().step()).map_err(engine_error)
    }

    /// Steps until every row has been fed. With `speed`, paced to that many times event time
    /// (60: an hour of rows in a minute); without, as fast as the graph runs.
    #[pyo3(signature = (speed=None))]
    fn run(&self, py: Python<'_>, speed: Option<f64>) -> PyResult<()> {
        loop {
            let done = py
                .detach(|| self.lock().run(speed, Some(CYCLES_PER_CHECK)))
                .map_err(engine_error)?;
            if done {
                return Ok(());
            }
            py.check_signals()?;
        }
    }

    /// The latest cycle's time, in nanoseconds since the epoch; None before the first.
    #[getter]
    fn time(&self) -> Option<i64> {
        self.lock().time()
    }

    /// Cycles run so far.
    #[getter]
    fn cycles(&self) -> u64 {
        self.lock().cycles()
    }

    /// Rows fed so far, across tables.
    #[getter]
    fn rows(&self) -> u64 {
        self.lock().rows()
    }

    /// The dates replayed, first and last.
    #[getter]
    fn dates(&self) -> (String, String) {
        (self.first.to_string(), self.last.to_string())
    }
}
