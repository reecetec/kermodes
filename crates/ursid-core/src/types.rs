use arrow::datatypes::{DataType, TimeUnit};

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
