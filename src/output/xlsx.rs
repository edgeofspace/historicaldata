use anyhow::Result;
use polars::prelude::*;
use rust_xlsxwriter::{Format, Workbook};

use super::reorder_columns;

/// Write flight data to Excel with Metadata, Ascent, and Descent sheets
pub fn write_xlsx(
    metadata_json: &serde_json::Value,
    ascent: &DataFrame,
    descent: &DataFrame,
    path: &str,
) -> Result<()> {
    let mut workbook = Workbook::new();

    // Datetime format matching Python's 'mm/dd/yy hh:mm:ss.000'
    let datetime_fmt = Format::new().set_num_format("mm/dd/yy hh:mm:ss.000");

    // Metadata sheet
    write_metadata_sheet(&mut workbook, metadata_json)?;

    // Reorder columns to standard order
    let ascent = reorder_columns(ascent)?;
    let descent = reorder_columns(descent)?;

    // Ascent sheet
    if ascent.height() > 0 {
        write_df_sheet(&mut workbook, "Ascent", &ascent, &datetime_fmt)?;
    }

    // Descent sheet
    if descent.height() > 0 {
        write_df_sheet(&mut workbook, "Descent", &descent, &datetime_fmt)?;
    }

    workbook.save(path)?;
    Ok(())
}

/// Write metadata as a flat key-value sheet
fn write_metadata_sheet(workbook: &mut Workbook, metadata: &serde_json::Value) -> Result<()> {
    let worksheet = workbook.add_worksheet();
    worksheet.set_name("Metadata")?;

    if let serde_json::Value::Object(map) = metadata {
        // Flatten the metadata into columns
        let flattened = flatten_json(map, "");

        // Write headers
        for (col, (key, _)) in flattened.iter().enumerate() {
            worksheet.write_string(0, col as u16, key)?;
        }

        // Write values
        for (col, (_, val)) in flattened.iter().enumerate() {
            match val {
                serde_json::Value::Number(n) => {
                    if let Some(f) = n.as_f64() {
                        worksheet.write_number(1, col as u16, f)?;
                    }
                }
                serde_json::Value::String(s) => {
                    worksheet.write_string(1, col as u16, s)?;
                }
                serde_json::Value::Bool(b) => {
                    worksheet.write_boolean(1, col as u16, *b)?;
                }
                serde_json::Value::Null => {}
                _ => {
                    worksheet.write_string(1, col as u16, &val.to_string())?;
                }
            }
        }
    }

    Ok(())
}

/// Flatten nested JSON into dot-separated key-value pairs
fn flatten_json(map: &serde_json::Map<String, serde_json::Value>, prefix: &str) -> Vec<(String, serde_json::Value)> {
    let mut result = Vec::new();
    for (key, val) in map {
        let full_key = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        match val {
            serde_json::Value::Object(inner) => {
                result.extend(flatten_json(inner, &full_key));
            }
            serde_json::Value::Array(arr) => {
                // Serialize arrays as JSON strings
                result.push((full_key, serde_json::Value::String(serde_json::to_string(arr).unwrap_or_default())));
            }
            _ => {
                result.push((full_key, val.clone()));
            }
        }
    }
    result
}

/// Write a DataFrame to an Excel worksheet
fn write_df_sheet(workbook: &mut Workbook, name: &str, df: &DataFrame, datetime_fmt: &Format) -> Result<()> {
    let worksheet = workbook.add_worksheet();
    worksheet.set_name(name)?;

    let col_names: Vec<String> = df.get_column_names().into_iter().map(|s| s.to_string()).collect();

    // Write headers
    for (col, name) in col_names.iter().enumerate() {
        worksheet.write_string(0, col as u16, name)?;
    }

    // Write data rows
    for row_idx in 0..df.height() {
        for (col_idx, col) in df.get_columns().iter().enumerate() {
            let s = col.as_materialized_series();
            let null_mask = s.is_null();
            if null_mask.get(row_idx).unwrap_or(false) {
                continue;
            }
            let xlsx_row = (row_idx + 1) as u32;
            let xlsx_col = col_idx as u16;

            match s.dtype() {
                DataType::Float64 => {
                    if let Some(v) = s.f64().ok().and_then(|ca| ca.get(row_idx)) {
                        worksheet.write_number(xlsx_row, xlsx_col, v)?;
                    }
                }
                DataType::Float32 => {
                    if let Some(v) = s.f32().ok().and_then(|ca| ca.get(row_idx)) {
                        worksheet.write_number(xlsx_row, xlsx_col, v as f64)?;
                    }
                }
                DataType::Int64 => {
                    if let Some(v) = s.i64().ok().and_then(|ca| ca.get(row_idx)) {
                        worksheet.write_number(xlsx_row, xlsx_col, v as f64)?;
                    }
                }
                DataType::Boolean => {
                    if let Some(v) = s.bool().ok().and_then(|ca| ca.get(row_idx)) {
                        worksheet.write_boolean(xlsx_row, xlsx_col, v)?;
                    }
                }
                DataType::String => {
                    if let Some(v) = s.str().ok().and_then(|ca| ca.get(row_idx)) {
                        worksheet.write_string(xlsx_row, xlsx_col, v)?;
                    }
                }
                DataType::Datetime(TimeUnit::Milliseconds, _) => {
                    if let Some(ms) = s.datetime().ok().and_then(|ca| ca.get(row_idx)) {
                        let secs = ms / 1000;
                        let nsecs = ((ms % 1000) * 1_000_000) as u32;
                        if let Some(dt) = chrono::DateTime::from_timestamp(secs, nsecs).map(|dt| dt.naive_utc()) {
                            let serial = naive_datetime_to_excel_serial(dt);
                            worksheet.write_number_with_format(xlsx_row, xlsx_col, serial, datetime_fmt)?;
                        }
                    }
                }
                _ => {
                    if let Ok(val) = s.get(row_idx) {
                        worksheet.write_string(xlsx_row, xlsx_col, &format!("{val}"))?;
                    }
                }
            }
        }
    }

    Ok(())
}

/// Convert NaiveDateTime to Excel serial date number
fn naive_datetime_to_excel_serial(dt: chrono::NaiveDateTime) -> f64 {
    // Excel epoch is 1899-12-30
    let excel_epoch = chrono::NaiveDate::from_ymd_opt(1899, 12, 30)
        .unwrap()
        .and_hms_opt(0, 0, 0)
        .unwrap();
    let duration = dt.signed_duration_since(excel_epoch);
    duration.num_milliseconds() as f64 / 86_400_000.0
}
