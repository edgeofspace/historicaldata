pub mod csv;
pub mod json;
pub mod kml;
pub mod metadata;
pub mod parquet;
pub mod xlsx;

use polars::prelude::*;

/// Standard column order for output
pub fn column_order() -> Vec<String> {
    vec![
        "flightid",
        "callsign",
        "receivetime",
        "packettime",
        "altitude_ft",
        "altitude_m",
        "vert_rate_ftmin",
        "elapsed_secs",
        "flight_phase",
        "position_packet",
        "info",
        "raw",
        "bearing",
        "speed_mph",
        "speed_kph",
        "latitude",
        "longitude",
        "distance_from_launch_mi",
        "distance_from_launch_km",
        "temperature_f",
        "temperature_c",
        "temperature_k",
        "pressure_pa",
        "pressure_atm",
        "airdensity_slugs",
        "airdensity_kgm3",
        "velocity_x_degs",
        "velocity_y_degs",
        "velocity_z_fts",
        "velocity_z_ms",
        "airflow",
        "acceleration_fts2",
        "velocity_mean_fts",
        "acceleration_mean_fts2",
        "velocity_std_fts",
        "acceleration_std_fts2",
        "velocity_norm_fts",
        "acceleration_norm_fts2",
        "velocity_curvefit_fts",
        "acceleration_ms2",
        "velocity_mean_ms",
        "acceleration_mean_ms2",
        "velocity_std_ms",
        "acceleration_std_ms2",
        "velocity_norm_ms",
        "acceleration_norm_ms2",
        "velocity_curvefit_ms",
    ]
    .into_iter()
    .map(String::from)
    .collect()
}

/// Reorder DataFrame columns to the standard order
pub fn reorder_columns(df: &DataFrame) -> anyhow::Result<DataFrame> {
    let order = column_order();
    let mut cols: Vec<Column> = Vec::new();
    for name in &order {
        if let Ok(col) = df.column(name.as_str()) {
            cols.push(col.clone());
        }
    }
    // Append remaining columns not in the standard order
    for col in df.get_columns() {
        if !order.contains(&col.name().to_string()) {
            cols.push(col.clone());
        }
    }
    Ok(DataFrame::new(cols)?)
}
