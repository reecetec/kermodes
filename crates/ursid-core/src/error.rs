use std::fmt;
use std::sync::Arc;

/// Everything that can go wrong in the engine, worded for whoever wrote the query.
#[derive(Debug, Clone, PartialEq)]
pub enum Error {
    /// An expression failed to parse or bind. `column` is 1-based within `text`.
    Expression {
        text: String,
        column: Option<u64>,
        message: String,
    },
    /// Data or a type name did not match what a table expects.
    Schema(String),
    /// Computing a table failed, while building it or during a cycle.
    Compute { table: String, message: String },
    /// A table failed because a table it reads from failed.
    Upstream { table: String, cause: Arc<Error> },
    /// The operation does not apply to this kind of table.
    Unsupported(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Expression {
                text,
                column,
                message,
            } => {
                write!(f, "{message}\n  {text}")?;
                if let Some(column) = column {
                    let pad = " ".repeat(usize::try_from(*column).unwrap_or(1).saturating_sub(1));
                    write!(f, "\n  {pad}^")?;
                }
                Ok(())
            }
            Error::Schema(message) | Error::Unsupported(message) => f.write_str(message),
            Error::Compute { table, message } => write!(f, "table `{table}` failed: {message}"),
            Error::Upstream { table, cause } => {
                write!(
                    f,
                    "table `{table}` failed because its input failed: {cause}"
                )
            }
        }
    }
}

impl std::error::Error for Error {}
