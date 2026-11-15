//! A stored append table shown without holding it (architecture.md §15.3): the grid's window
//! read from the store by position, its keys the rows' positions in the day, which never move.
//! What the server keeps is where the window is and the day's last row count.

use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use kermodes_core::viewer::{Frame, MARGIN};

/// Reads a stored table's day: its row count, and its rows `[start, start + count)` by position.
pub type ReadRows =
    Box<dyn Fn(&str, u64, u64) -> Result<(u64, Vec<RecordBatch>), String> + Send + Sync>;

#[derive(Clone)]
pub(crate) struct Window {
    pub(crate) name: String,
    schema: SchemaRef,
    /// The rows sent, `(start, count)`: as the engine's viewer sends them.
    range: (u64, u64),
    /// The rows on screen, once the grid has said.
    pub(crate) viewport: Option<(usize, usize)>,
}

impl Window {
    pub(crate) fn new(name: String, schema: SchemaRef) -> Self {
        Self {
            name,
            schema,
            range: (0, 2 * MARGIN as u64),
            viewport: None,
        }
    }

    /// The grid shows `top..=bottom`: send those and `MARGIN` either side.
    pub(crate) fn set_viewport(&mut self, top: usize, bottom: usize) {
        self.viewport = Some((top, bottom));
        let start = top.saturating_sub(MARGIN);
        self.range = (
            start as u64,
            (bottom.saturating_sub(top) + 1 + 2 * MARGIN) as u64,
        );
    }

    /// The window as the grid shows it now, read from the store.
    pub(crate) fn rows(&self, read: &ReadRows) -> Result<Frame, String> {
        let (start, count) = self.range;
        self.frame(read, None, start, count)
    }

    /// Exactly these rows, for copying a selection beyond the window.
    pub(crate) fn fetch(
        &self,
        read: &ReadRows,
        id: u64,
        start: u64,
        count: u64,
    ) -> Result<Frame, String> {
        self.frame(read, Some(id), start, count)
    }

    fn frame(
        &self,
        read: &ReadRows,
        fetched: Option<u64>,
        start: u64,
        count: u64,
    ) -> Result<Frame, String> {
        let (total, batches) = read(&self.name, start, count)?;
        let rows = concat_batches(&self.schema, &batches).map_err(|e| e.to_string())?;
        let mut ipc = Vec::new();
        let mut writer =
            StreamWriter::try_new(&mut ipc, &self.schema).map_err(|e| e.to_string())?;
        writer
            .write(&rows)
            .and_then(|()| writer.finish())
            .map_err(|e| e.to_string())?;
        drop(writer);
        let keys = (start..start + rows.num_rows() as u64)
            .flat_map(u64::to_le_bytes)
            .collect();
        Ok(Frame {
            fetched,
            start: start as usize,
            total: total as usize,
            // The day only grows: its row count says when the window may have changed.
            cycle: total,
            rows: ipc,
            keys,
        })
    }
}
