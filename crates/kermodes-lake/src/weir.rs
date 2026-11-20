//! What history needs of weir: the store in this process (`km.connect(path)`, `kermodes dev`), or the
//! cluster's over Flight (a merge Job, a worker reading history).

use std::time::Duration;

use arrow::array::RecordBatch;
use kermodes_flight::Client;
use kermodes_weir::{
    Date, Error, Garbage, HistoryCommit, HistoryDate, HistoryFile, Stamp, Store, TableEntry,
    TableName,
};

/// The `Store` methods of the same names.
pub trait Weir {
    fn entry(&self, name: &TableName) -> Result<TableEntry, Error>;
    fn date_rows(&self, name: &TableName, date: Date) -> Result<(u64, u64), Error>;
    fn read_date(
        &self,
        name: &TableName,
        date: Date,
        each: &mut dyn FnMut(RecordBatch, Stamp) -> Result<(), Error>,
    ) -> Result<(), Error>;
    fn past_dates(&self, name: &TableName) -> Result<Vec<Date>, Error>;
    fn history(&self, name: &TableName) -> Result<Vec<HistoryDate>, Error>;
    fn history_files(
        &self,
        name: &TableName,
        first: Date,
        last: Date,
    ) -> Result<Vec<HistoryFile>, Error>;
    fn commit_history(&self, name: &TableName, commit: &HistoryCommit) -> Result<(), Error>;
    fn drop_date(&self, name: &TableName, date: Date) -> Result<(), Error>;
    fn history_garbage(&self, grace: Duration) -> Result<Vec<Garbage>, Error>;
    fn forget_garbage(&self, garbage: &Garbage) -> Result<(), Error>;
}

impl Weir for Store {
    fn entry(&self, name: &TableName) -> Result<TableEntry, Error> {
        self.table(name)
    }

    fn date_rows(&self, name: &TableName, date: Date) -> Result<(u64, u64), Error> {
        Store::date_rows(self, name, date)
    }

    fn read_date(
        &self,
        name: &TableName,
        date: Date,
        each: &mut dyn FnMut(RecordBatch, Stamp) -> Result<(), Error>,
    ) -> Result<(), Error> {
        Store::read_date(self, name, date, each)
    }

    fn past_dates(&self, name: &TableName) -> Result<Vec<Date>, Error> {
        Store::past_dates(self, name)
    }

    fn history(&self, name: &TableName) -> Result<Vec<HistoryDate>, Error> {
        Store::history(self, name)
    }

    fn history_files(
        &self,
        name: &TableName,
        first: Date,
        last: Date,
    ) -> Result<Vec<HistoryFile>, Error> {
        Store::history_files(self, name, first, last)
    }

    fn commit_history(&self, name: &TableName, commit: &HistoryCommit) -> Result<(), Error> {
        Store::commit_history(self, name, commit)
    }

    fn drop_date(&self, name: &TableName, date: Date) -> Result<(), Error> {
        Store::drop_date(self, name, date)
    }

    fn history_garbage(&self, grace: Duration) -> Result<Vec<Garbage>, Error> {
        Store::history_garbage(self, grace)
    }

    fn forget_garbage(&self, garbage: &Garbage) -> Result<(), Error> {
        Store::forget_garbage(self, garbage)
    }
}

impl Weir for Client {
    fn entry(&self, name: &TableName) -> Result<TableEntry, Error> {
        Client::entry(self, name)
    }

    fn date_rows(&self, name: &TableName, date: Date) -> Result<(u64, u64), Error> {
        Client::date_rows(self, name, date)
    }

    fn read_date(
        &self,
        name: &TableName,
        date: Date,
        each: &mut dyn FnMut(RecordBatch, Stamp) -> Result<(), Error>,
    ) -> Result<(), Error> {
        Client::read_date(self, name, date, each)
    }

    fn past_dates(&self, name: &TableName) -> Result<Vec<Date>, Error> {
        Client::past_dates(self, name)
    }

    fn history(&self, name: &TableName) -> Result<Vec<HistoryDate>, Error> {
        Client::history(self, name)
    }

    fn history_files(
        &self,
        name: &TableName,
        first: Date,
        last: Date,
    ) -> Result<Vec<HistoryFile>, Error> {
        Client::history_files(self, name, first, last)
    }

    fn commit_history(&self, name: &TableName, commit: &HistoryCommit) -> Result<(), Error> {
        Client::commit_history(self, name, commit)
    }

    fn drop_date(&self, name: &TableName, date: Date) -> Result<(), Error> {
        Client::drop_date(self, name, date)
    }

    fn history_garbage(&self, grace: Duration) -> Result<Vec<Garbage>, Error> {
        Client::history_garbage(self, grace)
    }

    fn forget_garbage(&self, garbage: &Garbage) -> Result<(), Error> {
        Client::forget_garbage(self, garbage)
    }
}
