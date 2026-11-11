//! Names parsed once where they enter: table names (`namespace.table`) and ingest source IDs.

use std::fmt;

use crate::Error;

/// A table's name: `namespace.table`, both lowercase identifiers (`[a-z][a-z0-9_]*`, at most 63
/// characters); the `sys` namespace is the platform's.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct TableName {
    namespace: String,
    table: String,
}

impl TableName {
    pub fn parse(text: &str) -> Result<Self, Error> {
        let invalid = |why: &str| Error::Name(format!("`{text}` is not a table name: {why}"));
        let Some((namespace, table)) = text.split_once('.') else {
            return Err(invalid("expected `namespace.table`"));
        };
        for part in [namespace, table] {
            if !is_identifier(part) {
                return Err(invalid(
                    "each part is lowercase letters, digits, and `_`, starting with a letter",
                ));
            }
        }
        if namespace == "sys" {
            return Err(invalid("the `sys` namespace is reserved"));
        }
        Ok(Self {
            namespace: namespace.to_owned(),
            table: table.to_owned(),
        })
    }

    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    pub fn table(&self) -> &str {
        &self.table
    }
}

impl fmt::Display for TableName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}.{}", self.namespace, self.table)
    }
}

/// An ingest source within a table (one writer each): lowercase letters, digits, `_`, and `-`,
/// starting with a letter or digit, at most 63 characters. It names a directory.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct SourceId(String);

impl SourceId {
    pub fn parse(text: &str) -> Result<Self, Error> {
        let valid = (1..=63).contains(&text.len())
            && text.as_bytes()[0].is_ascii_alphanumeric()
            && text
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_' || b == b'-');
        if !valid {
            return Err(Error::Name(format!(
                "`{text}` is not a source id: lowercase letters, digits, `_`, and `-`, starting \
                 with a letter or digit"
            )));
        }
        Ok(Self(text.to_owned()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SourceId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn is_identifier(text: &str) -> bool {
    (1..=63).contains(&text.len())
        && text.as_bytes()[0].is_ascii_lowercase()
        && text
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
}
