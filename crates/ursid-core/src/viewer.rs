//! The table viewer's server side (architecture.md §15.3): one `Viewer` per open grid, whatever
//! carries its messages (the notebook widget's comm, the web UI's WebSocket).
//!
//! The grid says which rows it shows; the viewer answers with those rows and some either side,
//! read by position, as an Arrow IPC stream plus the rows' keys (which name a row wherever a sort
//! moves it). Sorting and quick filters build a table from the viewed one, `where` then `sort`,
//! which the viewer owns. The viewed table is held weakly, like every view: it goes when the code
//! that made it lets go, and `release` then drops the table built from it.

use std::collections::BTreeMap;

use arrow::array::types::{Date32Type, TimestampNanosecondType};
use arrow::compute::kernels::cast_utils::Parser;
use arrow::datatypes::DataType;
use arrow::error::ArrowError;
use arrow::ipc::writer::StreamWriter;

use crate::session::{Table, WeakTable};
use crate::{Error, type_name};

/// Rows sent beyond the visible ones on each side, so small scrolls need no round trip.
pub const MARGIN: usize = 100;
/// Most rows one fetch (a copy of a selection beyond the loaded window) returns.
pub const FETCH_LIMIT: usize = 100_000;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SortKey {
    pub column: String,
    pub descending: bool,
}

/// A message from the grid.
pub enum Request {
    /// The first and last rows on screen.
    Viewport { top: usize, bottom: usize },
    /// Sort keys, most significant first; empty for the table's own order.
    Sort(Vec<SortKey>),
    /// Quick-filter entries by column; empty entries are dropped.
    Filter(BTreeMap<String, String>),
    /// Exactly these rows, for copying a selection; answered as `Frame::fetched`.
    Fetch { id: u64, start: usize, count: usize },
}

/// Rows for the grid: the window's rows by position (Arrow IPC stream), their keys
/// (little-endian u64), and the shown table's row count.
pub struct Frame {
    /// The fetch this answers; `None` for the window the grid shows.
    pub fetched: Option<u64>,
    pub start: usize,
    pub total: usize,
    /// The viewed table's last changing cycle.
    pub cycle: u64,
    pub rows: Vec<u8>,
    pub keys: Vec<u8>,
}

pub struct Viewer {
    table: WeakTable,
    types: BTreeMap<String, DataType>,
    refreshing: bool,
    /// The sorted or filtered table shown instead, built from `table`; `None` when neither.
    shown: Option<Table>,
    window: (usize, usize),
    sort: Vec<SortKey>,
    filters: BTreeMap<String, String>,
    filter_errors: BTreeMap<String, String>,
}

impl Viewer {
    pub fn new(table: &Table) -> Self {
        Self {
            table: table.downgrade(),
            types: table
                .schema()
                .fields()
                .iter()
                .map(|f| (f.name().clone(), f.data_type().clone()))
                .collect(),
            refreshing: table.is_refreshing(),
            shown: None,
            window: (0, 2 * MARGIN),
            sort: Vec::new(),
            filters: BTreeMap::new(),
            filter_errors: BTreeMap::new(),
        }
    }

    /// Handles a message from the grid: the rows it should now show, or the rows it fetched.
    /// `None` once the viewed table is gone.
    pub fn receive(&mut self, request: Request) -> Result<Option<Frame>, Error> {
        match request {
            Request::Viewport { top, bottom } => {
                let start = top.saturating_sub(MARGIN);
                self.window = (start, bottom.saturating_sub(top) + 1 + 2 * MARGIN);
            }
            Request::Sort(sort) => self.derive(sort, self.filters.clone()),
            Request::Filter(filters) => {
                let filters = filters
                    .into_iter()
                    .filter(|(_, text)| !text.trim().is_empty())
                    .collect();
                self.derive(self.sort.clone(), filters);
            }
            Request::Fetch { id, start, count } => {
                return self.frame(Some(id), start, count.min(FETCH_LIMIT));
            }
        }
        self.rows()
    }

    /// The window the grid shows, as it is now; `None` once the viewed table is gone.
    pub fn rows(&self) -> Result<Option<Frame>, Error> {
        let (start, count) = self.window;
        self.frame(None, start, count)
    }

    /// The viewed table's last changing cycle; `None` once it is gone.
    pub fn changed(&self) -> Option<u64> {
        self.table.upgrade().map(|t| t.changed())
    }

    /// Drops the sorted or filtered table, so the engine can free it with the viewed table.
    pub fn release(&mut self) {
        self.shown = None;
    }

    pub fn sort(&self) -> &[SortKey] {
        &self.sort
    }

    pub fn filters(&self) -> &BTreeMap<String, String> {
        &self.filters
    }

    /// Why entries were not applied, by column.
    pub fn filter_errors(&self) -> &BTreeMap<String, String> {
        &self.filter_errors
    }

    /// "static", or "ticking · updated in cycle N".
    pub fn status(&self, cycle: u64) -> String {
        if self.refreshing {
            format!("ticking · updated in cycle {cycle}")
        } else {
            "static".to_owned()
        }
    }

    fn frame(
        &self,
        fetched: Option<u64>,
        start: usize,
        count: usize,
    ) -> Result<Option<Frame>, Error> {
        let Some(table) = self.table.upgrade() else {
            return Ok(None);
        };
        let shown = self.shown.as_ref().unwrap_or(&table);
        let window = shown.rows_at(start, count)?;
        let rows = ipc(&window.rows).map_err(|e| Error::Compute {
            table: shown.description(),
            message: e.to_string(),
        })?;
        Ok(Some(Frame {
            fetched,
            start,
            total: window.total,
            cycle: table.changed(),
            rows,
            keys: window.keys.iter().flat_map(|k| k.to_le_bytes()).collect(),
        }))
    }

    /// Rebuilds the shown table for `sort` and `filters`. Entries that are not filters are
    /// reported by column and skipped; if the engine refuses the rest, the table is shown as is.
    fn derive(&mut self, sort: Vec<SortKey>, filters: BTreeMap<String, String>) {
        let Some(table) = self.table.upgrade() else {
            return;
        };
        let mut errors = BTreeMap::new();
        let mut predicates = Vec::new();
        for (column, text) in &filters {
            let compiled = match self.types.get(column) {
                Some(data_type) => quick_filter(column, data_type, text),
                None => Err("no such column".to_owned()),
            };
            match compiled {
                Ok(predicate) => predicates.push(predicate),
                Err(message) => {
                    errors.insert(column.clone(), message);
                }
            }
        }
        let built = (|| {
            let mut shown = None;
            if !predicates.is_empty() {
                shown = Some(table.filter(&predicates.join(" AND "))?);
            }
            if !sort.is_empty() {
                let by: Vec<String> = sort
                    .iter()
                    .map(|key| match key.descending {
                        true => format!("{} desc", key.column),
                        false => key.column.clone(),
                    })
                    .collect();
                let by: Vec<&str> = by.iter().map(String::as_str).collect();
                shown = Some(shown.as_ref().unwrap_or(&table).sort(&by)?);
            }
            Ok::<_, Error>(shown)
        })();
        self.shown = match built {
            Ok(shown) => shown,
            Err(error) => {
                errors = filters
                    .keys()
                    .map(|c| (c.clone(), error.to_string()))
                    .collect();
                None
            }
        };
        self.sort = sort;
        self.filters = filters;
        self.filter_errors = errors;
    }
}

fn ipc(rows: &arrow::record_batch::RecordBatch) -> Result<Vec<u8>, ArrowError> {
    let mut bytes = Vec::new();
    let mut writer = StreamWriter::try_new(&mut bytes, &rows.schema())?;
    writer.write(rows)?;
    writer.finish()?;
    drop(writer);
    Ok(bytes)
}

/// The `where` predicate for a quick-filter entry on `column`, or why the entry is not one.
///
/// Every type takes `null` and `!null`. Strings: `text` (contains, any case), `=text`, `!=text`,
/// `!text` (does not contain). Numbers, dates, timestamps, and bools: a value, optionally after
/// `=`, `!=`, `>`, `>=`, `<`, `<=`. Values are checked here, so nothing but a literal reaches the
/// expression.
pub fn quick_filter(column: &str, data_type: &DataType, text: &str) -> Result<String, String> {
    let name = format!("\"{}\"", column.replace('"', "\"\""));
    let text = text.trim();
    if text.eq_ignore_ascii_case("null") {
        return Ok(format!("{name} IS NULL"));
    }
    if text.eq_ignore_ascii_case("!null") {
        return Ok(format!("{name} IS NOT NULL"));
    }
    if *data_type == DataType::Utf8 {
        return Ok(if let Some(value) = text.strip_prefix("!=") {
            format!("{name} <> {}", string(value))
        } else if let Some(value) = text.strip_prefix('=') {
            format!("{name} = {}", string(value))
        } else if let Some(value) = text.strip_prefix('!') {
            format!("{name} NOT ILIKE {}", string(&format!("%{value}%")))
        } else {
            format!("{name} ILIKE {}", string(&format!("%{text}%")))
        });
    }
    let (op, value) = [">=", "<=", "!=", "=", ">", "<"]
        .iter()
        .find_map(|op| text.strip_prefix(op).map(|rest| (*op, rest.trim())))
        .unwrap_or(("=", text));
    let op = if op == "!=" { "<>" } else { op };
    if value.is_empty() {
        return Err("needs a value".to_owned());
    }
    match data_type {
        DataType::Int32 | DataType::Int64 | DataType::Float64 => {
            if !is_number(value) {
                return Err(format!("`{value}` is not a number"));
            }
            Ok(format!("{name} {op} {value}"))
        }
        DataType::Boolean => match value.to_ascii_lowercase().as_str() {
            flag @ ("true" | "false") => Ok(format!("{name} {op} {flag}")),
            _ => Err("takes true, false, null, or !null".to_owned()),
        },
        DataType::Date32 | DataType::Timestamp(_, _) => {
            let parsed = match data_type {
                DataType::Date32 => Date32Type::parse(value).is_some(),
                _ => TimestampNanosecondType::parse(value).is_some(),
            };
            if !parsed {
                return Err(format!("`{value}` is not an ISO date or time"));
            }
            Ok(format!("{name} {op} {}", string(value)))
        }
        other => Err(format!("no quick filter for {} columns", type_name(other))),
    }
}

/// A decimal literal: digits with at most one point, an optional sign and exponent.
fn is_number(text: &str) -> bool {
    let digits = text.strip_prefix(['+', '-']).unwrap_or(text);
    let (mantissa, exponent) = match digits.split_once(['e', 'E']) {
        Some((m, e)) => (m, Some(e.strip_prefix(['+', '-']).unwrap_or(e))),
        None => (digits, None),
    };
    let (whole, fraction) = mantissa.split_once('.').unwrap_or((mantissa, ""));
    let all_digits = |s: &str| s.bytes().all(|b| b.is_ascii_digit());
    (!whole.is_empty() || !fraction.is_empty())
        && all_digits(whole)
        && all_digits(fraction)
        && exponent.is_none_or(|e| !e.is_empty() && all_digits(e))
}

fn string(text: &str) -> String {
    format!("'{}'", text.replace('\'', "''"))
}
