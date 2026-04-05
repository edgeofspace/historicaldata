use anyhow::{Context, Result};
use clap::Parser;
use polars::prelude::*;
use rayon::prelude::*;
use std::fs;

use eoss_processor::config::{Config, OutputType};
use eoss_processor::db;
use eoss_processor::models::{self, FlightInput, FlightMetadata, DetectedBurst, Location};
use eoss_processor::output;
use eoss_processor::processing;

fn main() -> Result<()> {
    let config = Config::parse();

    // Read and parse flightlist.json
    let flight_json = fs::read_to_string(&config.flightlist)
        .with_context(|| format!("Failed to read {}", config.flightlist))?;
    let flights: Vec<FlightInput> = serde_json::from_str(&flight_json)
        .with_context(|| format!("Failed to parse {}", config.flightlist))?;

    println!("Loaded {} flights from {}", flights.len(), config.flightlist);

    // Filter to single flight if specified
    let flights: Vec<FlightInput> = if let Some(ref name) = config.flight {
        let filtered: Vec<_> = flights
            .into_iter()
            .filter(|f| f.flight.eq_ignore_ascii_case(name))
            .collect();
        if filtered.is_empty() {
            anyhow::bail!("Flight '{}' not found in flight list", name);
        }
        filtered
    } else {
        flights
    };

    // Create output directories
    let subdirs = ["csv", "json", "kml", "xlsx", "parquet"];
    for sub in &subdirs {
        let dir = format!("{}/{}", config.output_dir, sub);
        fs::create_dir_all(&dir)
            .with_context(|| format!("Failed to create directory {dir}"))?;
    }

    // Process flights in parallel with rayon
    let results: Vec<Result<()>> = flights
        .par_iter()
        .map(|flight| process_flight(flight, &config))
        .collect();

    // Report errors
    let mut success_count = 0;
    for result in &results {
        match result {
            Ok(()) => success_count += 1,
            Err(e) => eprintln!("Error: {e:#}"),
        }
    }

    // Consolidate metadata
    let should_consolidate = flights.len() > 1
        || config.output_type == OutputType::All
        || config.output_type == OutputType::Json
        || config.output_type == OutputType::Csv
        || config.output_type == OutputType::Xlsx;

    if should_consolidate && success_count > 0 {
        println!("Consolidating flight metadata...");
        if let Err(e) = output::metadata::write_consolidated_metadata(&config.output_dir) {
            eprintln!("Error consolidating metadata: {e:#}");
        }
    }

    println!("Flights processed: {success_count}/{}", flights.len());

    Ok(())
}

fn process_flight(input: &FlightInput, config: &Config) -> Result<()> {
    let flightname = &input.flight;

    // Unit conversions
    let (weights, parachute) = models::convert_units(input);

    // Query database
    let df = db::query_database(config, input)
        .with_context(|| format!("Database query failed for {flightname}"))?;

    // Process data
    let processed = processing::process_df(flightname, df)
        .with_context(|| format!("Processing failed for {flightname}"))?;

    // Build consolidated DataFrame (ascent + descent)
    // Align columns before vstacking — curve fit columns may only exist in one phase
    let consolidated = if processed.descent.height() > 0 {
        let mut ascent = processed.ascent.clone();
        let mut descent = processed.descent.clone();

        // Add missing columns as null f64 to each frame
        let desc_cols: Vec<PlSmallStr> = descent.get_column_names().into_iter().cloned().collect();
        for col_name in &desc_cols {
            if ascent.column(col_name.as_str()).is_err() {
                let null_col = Column::new(col_name.clone(), vec![Option::<f64>::None; ascent.height()]);
                ascent.with_column(null_col)?;
            }
        }
        let asc_cols: Vec<PlSmallStr> = ascent.get_column_names().into_iter().cloned().collect();
        for col_name in &asc_cols {
            if descent.column(col_name.as_str()).is_err() {
                let null_col = Column::new(col_name.clone(), vec![Option::<f64>::None; descent.height()]);
                descent.with_column(null_col)?;
            }
        }

        // Reorder descent columns to match ascent
        let col_order = asc_cols;
        let descent = descent.select(col_order)?;

        let mut frames = ascent;
        frames.vstack_mut(&descent)?;
        frames.sort(["packettime"], SortMultipleOptions::default())?
    } else {
        processed.ascent.clone()
    };

    // Build metadata
    let first_row_lat = consolidated.column("latitude")?.f64()?.get(0).unwrap_or(0.0);
    let first_row_lon = consolidated.column("longitude")?.f64()?.get(0).unwrap_or(0.0);
    let first_row_alt_ft = consolidated.column("altitude_ft")?.f64()?.get(0).unwrap_or(0.0);
    let first_row_alt_m = consolidated.column("altitude_m")?.f64()?.get(0).unwrap_or(0.0);

    let last_idx = consolidated.height() - 1;
    let last_row_lat = consolidated.column("latitude")?.f64()?.get(last_idx).unwrap_or(0.0);
    let last_row_lon = consolidated.column("longitude")?.f64()?.get(last_idx).unwrap_or(0.0);
    let last_row_alt_ft = consolidated.column("altitude_ft")?.f64()?.get(last_idx).unwrap_or(0.0);
    let last_row_alt_m = consolidated.column("altitude_m")?.f64()?.get(last_idx).unwrap_or(0.0);
    let last_dist_mi = consolidated.column("distance_from_launch_mi")?.f64()?.get(last_idx).unwrap_or(0.0);
    let last_dist_km = consolidated.column("distance_from_launch_km")?.f64()?.get(last_idx).unwrap_or(0.0);

    // Flight time
    let pt = consolidated.column("packettime")?.datetime()?;
    let start_ms = pt.get(0).unwrap_or(0);
    let end_ms = pt.get(last_idx).unwrap_or(0);
    let flighttime_seconds = (end_ms - start_ms) as f64 / 1000.0;
    let hrs = (flighttime_seconds / 3600.0) as i64;
    let mins = ((flighttime_seconds - hrs as f64 * 3600.0) / 60.0) as i64;
    let secs = (flighttime_seconds - hrs as f64 * 3600.0 - mins as f64 * 60.0) as i64;
    let flighttime = format!("{hrs}hrs {mins}mins {secs}secs");

    let detected_burst = if let Some(burst_alt) = processed.detected_burst {
        DetectedBurst {
            detected: true,
            burst_ft: burst_alt as f64,
            burst_m: (burst_alt as f64 * 0.3048 * 100.0).round() / 100.0,
        }
    } else {
        DetectedBurst {
            detected: false,
            burst_ft: 0.0,
            burst_m: 0.0,
        }
    };

    let metadata = FlightMetadata {
        flight: flightname.clone(),
        beacons: input.beacons.clone(),
        day: input.day.clone(),
        balloonsize: input.balloonsize.clone(),
        parachute,
        weights,
        liftfactor: input.liftfactor.clone(),
        h2fill: input.h2fill.clone(),
        maxaltitude_ft: processed.max_altitude_ft,
        maxaltitude_m: processed.max_altitude_m,
        detected_burst,
        numpoints: consolidated.height(),
        flighttime,
        flighttime_secs: flighttime_seconds,
        range_distance_traveled_mi: last_dist_mi,
        range_distance_traveled_km: last_dist_km,
        launch_location: Location {
            latitude: first_row_lat,
            longitude: first_row_lon,
            altitude_ft: first_row_alt_ft,
            altitude_m: first_row_alt_m,
            distance_from_launch_mi: None,
            distance_from_launch_km: None,
        },
        landing_location: Location {
            latitude: last_row_lat,
            longitude: last_row_lon,
            altitude_ft: last_row_alt_ft,
            altitude_m: last_row_alt_m,
            distance_from_launch_mi: Some(last_dist_mi),
            distance_from_launch_km: Some(last_dist_km),
        },
        reynolds_transitions: processed.reynolds_transitions,
    };

    let flight_lower = flightname.to_lowercase();
    let out = &config.output_dir;
    let otype = &config.output_type;

    // Write outputs based on --output-type flag
    if *otype == OutputType::All || *otype == OutputType::Csv {
        output::csv::write_csv(&consolidated, &format!("{out}/csv/{flight_lower}.csv"))?;

        // Individual metadata CSV
        let meta_val = serde_json::to_value(&metadata)?;
        if let serde_json::Value::Object(map) = &meta_val {
            let flat = flatten_metadata(map);
            let keys: Vec<&str> = flat.iter().map(|(k, _)| k.as_str()).collect();
            let vals: Vec<String> = flat.iter().map(|(_, v)| format_csv_value(v)).collect();
            let csv_content = format!("{}\n{}\n", keys.join(","), vals.join(","));
            fs::write(format!("{out}/csv/{flight_lower}_metadata.csv"), csv_content)?;
        }
    }

    if *otype == OutputType::All || *otype == OutputType::Json {
        output::json::write_json(&metadata, &consolidated, &format!("{out}/json/{flight_lower}.json"))?;
    }

    if *otype == OutputType::All || *otype == OutputType::Parquet {
        output::parquet::write_parquet(&consolidated, &format!("{out}/parquet/{flight_lower}.parquet"))?;
    }

    if *otype == OutputType::All || *otype == OutputType::Xlsx {
        let meta_val = serde_json::to_value(&metadata)?;
        output::xlsx::write_xlsx(&meta_val, &processed.ascent, &processed.descent, &format!("{out}/xlsx/{flight_lower}.xlsx"))?;
    }

    if *otype == OutputType::All || *otype == OutputType::Kml {
        if processed.ascent.height() > 0 && processed.descent.height() > 0 {
            output::kml::write_kml(
                &flightname.to_uppercase(),
                &processed.ascent,
                &processed.descent,
                &format!("{out}/kml/{flight_lower}.kml"),
            )?;
        }
    }

    Ok(())
}

fn flatten_metadata(map: &serde_json::Map<String, serde_json::Value>) -> Vec<(String, serde_json::Value)> {
    let mut result = Vec::new();
    for (key, val) in map {
        match val {
            serde_json::Value::Object(inner) => {
                for (k, v) in flatten_metadata(inner) {
                    result.push((format!("{key}.{k}"), v));
                }
            }
            serde_json::Value::Array(arr) => {
                result.push((key.clone(), serde_json::Value::String(serde_json::to_string(arr).unwrap_or_default())));
            }
            _ => {
                result.push((key.clone(), val.clone()));
            }
        }
    }
    result
}

fn format_csv_value(val: &serde_json::Value) -> String {
    match val {
        serde_json::Value::String(s) => {
            if s.contains(',') || s.contains('"') || s.contains('\n') {
                format!("\"{}\"", s.replace('"', "\"\""))
            } else {
                s.clone()
            }
        }
        serde_json::Value::Null => String::new(),
        _ => val.to_string(),
    }
}
