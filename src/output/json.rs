use anyhow::Result;
use polars::prelude::*;
use serde_json::{Map, Value, json};
use std::fs;

use crate::models::FlightMetadata;
use super::reorder_columns;

/// Write flight data as nested JSON (metadata + packets array)
pub fn write_json(metadata: &FlightMetadata, df: &DataFrame, path: &str) -> Result<()> {
    let df = reorder_columns(df)?;
    // Serialize metadata to a JSON map
    let meta_val = serde_json::to_value(metadata)?;
    let mut root = match meta_val {
        Value::Object(m) => m,
        _ => Map::new(),
    };

    // Convert DataFrame rows to JSON array
    let packets = df_to_json_array(&df)?;
    root.insert("packets".to_string(), Value::Array(packets));

    let json_str = serde_json::to_string(&root)?;
    fs::write(path, json_str)?;
    Ok(())
}

/// Convert a DataFrame to a Vec of JSON objects
fn df_to_json_array(df: &DataFrame) -> Result<Vec<Value>> {
    let nrows = df.height();
    let ncols = df.width();
    let col_names: Vec<String> = df.get_column_names().into_iter().map(|s| s.to_string()).collect();

    let mut rows: Vec<Value> = Vec::with_capacity(nrows);

    for i in 0..nrows {
        let mut obj = Map::new();
        for j in 0..ncols {
            let col = df.get_columns()[j].as_materialized_series();
            let name = &col_names[j];
            let val = series_value_at(col, i);
            obj.insert(name.clone(), val);
        }
        rows.push(Value::Object(obj));
    }

    Ok(rows)
}

/// Extract a single value from a Series at index i as a serde_json::Value
fn series_value_at(s: &Series, i: usize) -> Value {
    let null_mask = s.is_null();
    if null_mask.get(i).unwrap_or(false) {
        return Value::Null;
    }

    match s.dtype() {
        DataType::Float64 => {
            s.f64().ok().and_then(|ca| ca.get(i)).map_or(Value::Null, |v| json!(v))
        }
        DataType::Float32 => {
            s.f32().ok().and_then(|ca| ca.get(i)).map_or(Value::Null, |v| json!(v))
        }
        DataType::Int64 => {
            s.i64().ok().and_then(|ca| ca.get(i)).map_or(Value::Null, |v| json!(v))
        }
        DataType::Int32 => {
            s.i32().ok().and_then(|ca| ca.get(i)).map_or(Value::Null, |v| json!(v))
        }
        DataType::Boolean => {
            s.bool().ok().and_then(|ca| ca.get(i)).map_or(Value::Null, |v| json!(v))
        }
        DataType::String => {
            s.str().ok().and_then(|ca| ca.get(i)).map_or(Value::Null, |v| json!(v))
        }
        DataType::Datetime(TimeUnit::Milliseconds, _) => {
            s.datetime()
                .ok()
                .and_then(|ca| ca.get(i))
                .map_or(Value::Null, |ms| {
                    // Convert milliseconds since epoch to ISO 8601
                    let secs = ms / 1000;
                    let nsecs = ((ms % 1000) * 1_000_000) as u32;
                    if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs).map(|dt| dt.naive_utc()) {
                        json!(dt.format("%Y-%m-%dT%H:%M:%S%.3f").to_string())
                    } else {
                        Value::Null
                    }
                })
        }
        _ => {
            // Fallback: use debug format
            let val = s.get(i).ok();
            val.map_or(Value::Null, |v| json!(format!("{v}")))
        }
    }
}
