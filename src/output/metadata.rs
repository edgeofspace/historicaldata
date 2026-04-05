use anyhow::Result;
use std::fs;

/// Consolidate per-flight JSON files into flights_metadata outputs
pub fn write_consolidated_metadata(output_dir: &str) -> Result<()> {
    let json_dir = format!("{output_dir}/json");
    let csv_dir = format!("{output_dir}/csv");
    let xlsx_dir = format!("{output_dir}/xlsx");

    let mut flight_metadata: Vec<serde_json::Value> = Vec::new();

    // Read all individual flight JSON files
    let mut entries: Vec<_> = fs::read_dir(&json_dir)?
        .filter_map(|e| e.ok())
        .filter(|e| {
            let name = e.file_name().to_string_lossy().to_string();
            name.ends_with(".json") && name != "flights_metadata.json"
        })
        .collect();
    entries.sort_by_key(|e| e.file_name());

    for entry in entries {
        let content = fs::read_to_string(entry.path())?;
        let mut json_data: serde_json::Value = serde_json::from_str(&content)?;

        // Remove packets key
        if let serde_json::Value::Object(ref mut map) = json_data {
            map.remove("packets");
        }

        flight_metadata.push(json_data);
    }

    if flight_metadata.is_empty() {
        return Ok(());
    }

    // Write consolidated JSON
    let json_path = format!("{json_dir}/flights_metadata.json");
    fs::write(&json_path, serde_json::to_string(&flight_metadata)?)?;

    // Write consolidated CSV
    // Flatten each flight's metadata into a row
    let flattened: Vec<serde_json::Value> = flight_metadata
        .iter()
        .map(|v| {
            if let serde_json::Value::Object(map) = v {
                serde_json::Value::Object(flatten_for_csv(map, ""))
            } else {
                v.clone()
            }
        })
        .collect();

    // Collect all unique keys
    let mut all_keys: Vec<String> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for item in &flattened {
        if let serde_json::Value::Object(map) = item {
            for key in map.keys() {
                if seen.insert(key.clone()) {
                    all_keys.push(key.clone());
                }
            }
        }
    }

    // Build CSV content
    let mut csv_content = all_keys.join(",") + "\n";
    for item in &flattened {
        if let serde_json::Value::Object(map) = item {
            let row: Vec<String> = all_keys
                .iter()
                .map(|key| {
                    map.get(key)
                        .map(|v| match v {
                            serde_json::Value::String(s) => {
                                if s.contains(',') || s.contains('"') || s.contains('\n') {
                                    format!("\"{}\"", s.replace('"', "\"\""))
                                } else {
                                    s.clone()
                                }
                            }
                            serde_json::Value::Null => String::new(),
                            _ => v.to_string(),
                        })
                        .unwrap_or_default()
                })
                .collect();
            csv_content.push_str(&row.join(","));
            csv_content.push('\n');
        }
    }

    let csv_path = format!("{csv_dir}/flights_metadata.csv");
    fs::write(&csv_path, &csv_content)?;

    // Write consolidated XLSX
    let xlsx_path = format!("{xlsx_dir}/flights_metadata.xlsx");
    let mut workbook = rust_xlsxwriter::Workbook::new();
    let worksheet = workbook.add_worksheet();
    worksheet.set_name("Flightlist")?;

    // Headers
    for (col, key) in all_keys.iter().enumerate() {
        worksheet.write_string(0, col as u16, key)?;
    }

    // Data rows
    for (row_idx, item) in flattened.iter().enumerate() {
        if let serde_json::Value::Object(map) = item {
            for (col_idx, key) in all_keys.iter().enumerate() {
                let xlsx_row = (row_idx + 1) as u32;
                let xlsx_col = col_idx as u16;
                if let Some(val) = map.get(key) {
                    match val {
                        serde_json::Value::Number(n) => {
                            if let Some(f) = n.as_f64() {
                                worksheet.write_number(xlsx_row, xlsx_col, f)?;
                            }
                        }
                        serde_json::Value::String(s) => {
                            worksheet.write_string(xlsx_row, xlsx_col, s)?;
                        }
                        serde_json::Value::Bool(b) => {
                            worksheet.write_boolean(xlsx_row, xlsx_col, *b)?;
                        }
                        serde_json::Value::Null => {}
                        _ => {
                            worksheet.write_string(xlsx_row, xlsx_col, &val.to_string())?;
                        }
                    }
                }
            }
        }
    }

    workbook.save(&xlsx_path)?;

    Ok(())
}

/// Flatten nested JSON object into dot-separated keys for CSV/XLSX
fn flatten_for_csv(
    map: &serde_json::Map<String, serde_json::Value>,
    prefix: &str,
) -> serde_json::Map<String, serde_json::Value> {
    let mut result = serde_json::Map::new();
    for (key, val) in map {
        let full_key = if prefix.is_empty() {
            key.clone()
        } else {
            format!("{prefix}.{key}")
        };
        match val {
            serde_json::Value::Object(inner) => {
                let flat = flatten_for_csv(inner, &full_key);
                result.extend(flat);
            }
            serde_json::Value::Array(arr) => {
                result.insert(
                    full_key,
                    serde_json::Value::String(serde_json::to_string(arr).unwrap_or_default()),
                );
            }
            _ => {
                result.insert(full_key, val.clone());
            }
        }
    }
    result
}
