# Architecture Guide

This document provides a technical overview of the `eoss-processor` Rust application for future developers.

## Overview

`eoss-processor` reads historical EOSS high-altitude balloon flight data from a PostgreSQL/PostGIS database, processes raw APRS telemetry packets into enriched flight datasets, and produces multiple output formats. The application supports parallel processing of multiple flights via rayon.

## Module Layout

```
src/
  main.rs          CLI entry point, orchestration, output dispatch
  config.rs        CLI argument definitions (clap)
  models.rs        Data structures for flight input/output, unit conversions
  db.rs            PostgreSQL queries, DST detection, DataFrame construction
  processing.rs    Core analysis pipeline (trimming, physics, Reynolds detection, curve fitting)
  physics.rs       Haversine distance, air density, polynomial fitting (nalgebra SVD)
  output/
    mod.rs         Standard column ordering and reorder utility
    csv.rs         CSV output via Polars CsvWriter
    json.rs        Nested JSON (metadata + packets array)
    parquet.rs     Parquet output via Polars ParquetWriter
    xlsx.rs        Multi-sheet Excel (Metadata, Ascent, Descent)
    kml.rs         Google Earth KML with 3D paths, waypoints, and points of interest
    metadata.rs    Consolidated cross-flight metadata (JSON, CSV, XLSX)
```

## Processing Pipeline

Each flight goes through the following stages in `process_flight()` (main.rs) and `process_df()` (processing.rs):

### 1. Database Query (`db.rs`)

The SQL query in `QUERY_SQL` extracts APRS packets from the `packets` table for a given flight day and set of beacon callsigns. Key aspects:

- **Timezone handling**: `is_dst()` determines whether the flight date falls in MDT or MST by checking the UTC offset at 03:00 local Denver time. The query window spans 03:00-23:59 local time, converted to UTC for the `WHERE` clause.
- **Packet time extraction**: The SQL parses APRS-encoded timestamps from the raw packet string when available (format `HHMMSSh`), falling back to the database receive time.
- **Telemetry parsing**: Temperature and pressure are extracted from raw packet strings using SQL regex patterns matching the `<temp>T<pressure>P` APRS telemetry encoding. Temperature is returned in Fahrenheit, Celsius, and Kelvin; pressure in atmospheres and Pascals.
- **Connection mode**: Without `--dbhost`, connects via Unix socket (peer auth). With `--dbhost`, connects via TCP with optional user/password.

The query returns a Polars DataFrame with 17 columns including position, altitude, speed, bearing, and parsed telemetry.

### 2. Burst Detection (`processing.rs:29-46`)

Before any rows are dropped, the raw packet strings are scanned for burst/commanded-release status messages using regex. These packets typically lack position data and would be lost after null filtering. The highest reported burst altitude is extracted and stored in metadata.

### 3. Per-Beacon Processing (`processing.rs:92-193`)

Each beacon (callsign) is processed independently, then concatenated:

- Filter to rows with valid position data (`altitude_ft > 0`, non-null lat/lon)
- Sort by packet time and deduplicate timestamps
- Compute time deltas between consecutive packets
- Compute velocities: `velocity_x_degs` (lon/s), `velocity_y_degs` (lat/s), `velocity_z_fts` (ft/s), `velocity_z_ms` (m/s), `vert_rate_ftmin` (ft/min)
- Compute air density in both slugs/ft^3 and kg/m^3 from pressure and temperature

### 4. Packet Trimming

Trimming removes pre-launch ground packets and post-landing straggler packets. This is critical because raw APRS data includes hours of ground-level telemetry before launch and scattered packets after landing.

#### 4a. Straggler Removal (`processing.rs:243-261`)

Post-landing stragglers are identified first. A packet is considered a straggler when all three conditions are met:
- `timedelta_s > 500` (more than ~8 minutes since previous packet)
- `altitude_ft < 8000` (near ground level)
- `ascending == false` (in the descent phase)

Everything after the first straggler packet is truncated.

#### 4b. Outlier Ejection (`processing.rs:263-277`)

Velocity outliers are removed:
- **Ascending packets**: keep only where `-5 < velocity_z_fts < 50` (ft/s)
- **Descending packets**: keep only where `velocity_z_fts < 5` (ft/s)

#### 4c. Forward Moving Averages (`processing.rs:288-298`, `779-804`)

Two forward moving averages of vertical velocity are computed using a reverse-rolling-reverse technique:
- **Short window** (3 points): `forward_avg`
- **Long window** (10 points): `long_forward_avg`

The "forward" aspect means each point's average looks ahead in time, not behind. This is implemented by reversing the array, computing a standard rolling mean, then reversing the result.

#### 4d. Launch Detection (`processing.rs:300-319`)

Scans forward through the data looking for the first packet where all conditions hold:
- `velocity_z_fts > 0` (moving upward)
- `forward_avg > 5` (sustained upward trend in short window)
- `long_forward_avg > 5` (sustained upward trend in long window)
- `ascending == true`
- `altitude_ft < 8000` (still near launch altitude)

The trim point is set one packet before this detection point (to include the last ground packet).

#### 4e. Landing Detection (`processing.rs:321-334`)

Scans forward looking for the first packet after the launch point where:
- `velocity_z_fts > -5` (near-zero descent rate)
- `forward_avg > -5` (short average confirms slowdown)
- `long_forward_avg > -5` (long average confirms slowdown)
- `ascending == false` (in descent phase)
- `altitude_ft < 8000` (near ground level)

The trim point is set one packet after this detection point.

### 5. Ascent/Descent Split (`processing.rs:370-392`)

After trimming, the maximum altitude index is recalculated. Packets at or before the max altitude index are flagged `ascending = true`; those after are `ascending = false`. The DataFrame is split into separate ascent and descent DataFrames.

### 6. Phase Processing (`process_phase`, `processing.rs:644-776`)

Applied independently to both ascent and descent:

- **Elapsed seconds**: time since phase start
- **Distance-to-line**: perpendicular distance from each point to the straight line connecting the first and last points of the phase (in elapsed_secs vs altitude_ft space). Used for Reynolds transition detection on ascent. See `physics::distance_to_line`.
- **Acceleration**: computed from consecutive velocity differences divided by time delta
- **Expanding statistics**: cumulative (expanding window) mean and standard deviation for velocity and acceleration, used for normalization
- **Normalized values**: `(value - expanding_mean) / expanding_std` for velocity and acceleration

### 7. Reynolds Transition Detection (`processing.rs:401-452`)

Reynolds transitions mark altitude boundaries where balloon airflow shifts between laminar (low Re) and turbulent (high Re) regimes. Detection uses the distance-to-line values from the ascent phase:

- Only considers points above 10,000 ft altitude
- Finds the point with maximum positive distance-to-line (> 30 ft threshold) and maximum negative distance-to-line (< -30 ft threshold)
- The relative ordering of these extremes determines the airflow labeling pattern (e.g., high Re -> low Re -> high Re)
- Transition points where the airflow label changes are recorded in metadata

### 8. Polynomial Curve Fitting (`processing.rs:466-498`, `physics.rs:54-109`)

Polynomial curves are fit to velocity vs. altitude for both phases:

- **Degree selection**: The polynomial degree is chosen adaptively using the Variance-to-Mean Ratio (VMR) of vertical velocity (`physics::vmr_degree`). Higher variance relative to mean yields a lower degree (smoother fit). Maximum degree is 13.
- **Fitting method**: `physics::polynomial_fit` maps x-values to [-1, 1] for numerical stability (matching numpy's `Polynomial.fit` behavior), builds a Vandermonde matrix, and solves via SVD least-squares using nalgebra.
- **Output**: The fit produces `velocity_curvefit_fts` and `velocity_curvefit_ms` columns in both imperial and metric units.

### 9. Output Generation

After processing, ascent and descent DataFrames are recombined (with column alignment for any columns that only exist in one phase) and sorted by packet time.

#### Column Ordering (`output/mod.rs`)

All output formats share a standard column order defined in `column_order()`. The `reorder_columns()` utility reorders DataFrame columns to this standard, appending any extra columns not in the standard list.

#### CSV (`output/csv.rs`)

Two CSV files per flight:
- `{flight}.csv` — full packet data with standard column ordering
- `{flight}_metadata.csv` — single-row flattened metadata (generated in main.rs)

#### JSON (`output/json.rs`)

Nested JSON with flight metadata at the top level and a `packets` array containing all rows. DateTime values are formatted as ISO 8601 strings. The `series_value_at` function handles type-specific extraction from Polars Series.

#### Parquet (`output/parquet.rs`)

Standard Polars Parquet output with column reordering. Intended as the machine-readable format for downstream analysis.

#### Excel (`output/xlsx.rs`)

Multi-sheet workbook:
- **Metadata** sheet: flattened key-value pairs from flight metadata
- **Ascent** sheet: full ascent DataFrame
- **Descent** sheet: full descent DataFrame

DateTime columns are written as Excel serial date numbers with format `mm/dd/yy hh:mm:ss.000`. The conversion (`naive_datetime_to_excel_serial`) uses the Excel epoch of 1899-12-30.

#### KML (`output/kml.rs`)

Google Earth visualization with:
- **Paths folder**: 3D LineString placemarks for ascent (red) and descent (blue), using absolute altitude mode
- **Waypoints folder**: placemarks at 10,000 ft altitude intervals for both phases
- **Points of Interest folder**: Launch, Landing, Burst, and Reynolds transition points with HTML description popups

The descent path is prepended with the burst point to create a continuous line from burst to landing.

#### Consolidated Metadata (`output/metadata.rs`)

After all flights are processed, `write_consolidated_metadata` reads the individual JSON output files (excluding the packets arrays), and writes:
- `flights_metadata.json` — array of all flight metadata objects
- `flights_metadata.csv` — flattened tabular format
- `flights_metadata.xlsx` — Excel workbook with a Flightlist sheet

## Key Functions Reference

| Function | Location | Purpose |
|----------|----------|---------|
| `process_df` | `processing.rs:26` | Main per-flight processing pipeline |
| `process_phase` | `processing.rs:644` | Per-phase (ascent/descent) column computation |
| `compute_forward_avg` | `processing.rs:779` | Forward-looking moving average via reverse-roll-reverse |
| `query_database` | `db.rs:114` | SQL query execution and DataFrame construction |
| `is_dst` | `db.rs:12` | DST detection for Mountain timezone |
| `polynomial_fit` | `physics.rs:54` | Numerically stable polynomial fitting via SVD |
| `vmr_degree` | `physics.rs:37` | Adaptive polynomial degree selection |
| `haversine_distance` | `physics.rs:4` | Great-circle distance between coordinates |
| `distance_to_line` | `physics.rs:32` | Signed perpendicular distance from point to line |
| `reorder_columns` | `output/mod.rs` | Standardize DataFrame column ordering |
| `write_consolidated_metadata` | `output/metadata.rs:5` | Cross-flight metadata aggregation |
