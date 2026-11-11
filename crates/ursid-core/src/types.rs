use arrow::datatypes::{DataType, Schema, TimeUnit};

use crate::Error;

const NAMES: &str = "bool, int32, int64, float64, utf8, date32, timestamp";

/// Parses a column type name as used by the Python API.
pub fn parse_type(name: &str) -> Result<DataType, Error> {
    Ok(match name {
        "bool" => DataType::Boolean,
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "float64" => DataType::Float64,
        "utf8" => DataType::Utf8,
        "date32" => DataType::Date32,
        "timestamp" => DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        other => {
            return Err(Error::Schema(format!(
                "unknown column type `{other}`; expected one of {NAMES}"
            )));
        }
    })
}

/// Rejects columns outside the engine's types: every table's columns have one of the types
/// `parse_type` names.
pub(crate) fn check_types(schema: &Schema) -> Result<(), Error> {
    for field in schema.fields() {
        let data_type = field.data_type();
        let supported = matches!(
            data_type,
            DataType::Boolean
                | DataType::Int32
                | DataType::Int64
                | DataType::Float64
                | DataType::Utf8
                | DataType::Date32
        ) || matches!(data_type, DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)) if tz.as_ref() == "UTC");
        if !supported {
            return Err(Error::Schema(format!(
                "column `{}` is {}; supported types are {NAMES}",
                field.name(),
                type_name(data_type)
            )));
        }
    }
    Ok(())
}

/// The name `parse_type` accepts for a type, or Arrow's own name for types outside that set.
pub fn type_name(data_type: &DataType) -> String {
    match data_type {
        DataType::Boolean => "bool".into(),
        DataType::Int32 => "int32".into(),
        DataType::Int64 => "int64".into(),
        DataType::Float64 => "float64".into(),
        DataType::Utf8 => "utf8".into(),
        DataType::Date32 => "date32".into(),
        DataType::Timestamp(TimeUnit::Nanosecond, Some(tz)) if tz.as_ref() == "UTC" => {
            "timestamp".into()
        }
        DataType::Null => "null".into(),
        other => other.to_string(),
    }
}
