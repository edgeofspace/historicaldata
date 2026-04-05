# EOSS Historical Flight Data Processor

A command-line application that processes historical [Edge of Space Sciences](https://eoss.org) (EOSS) high-altitude balloon flight data. It reads raw APRS telemetry packets from a PostgreSQL database, analyzes the flight profile (velocities, accelerations, air density, Reynolds number transitions), and produces output files in several formats.

Additionally, flight data is included in the `output` folder and contains cleaned/processed datasets for nearly all EOSS flights from EOSS-291 through the present.  To preview flight data live, visit [Historical Data](https://track.eoss.org/historical.php).

## What This Utility Does

For each flight in the flight list, the application:

1. Queries the APRS packet database for all telemetry from the flight's beacons on the flight day
2. Trims pre-launch ground packets and post-landing stragglers using moving-average heuristics
3. Splits the flight into ascent and descent phases at the maximum altitude point
4. Computes derived columns: vertical velocity, acceleration, air density, distance from launch, polynomial curve fits, and Reynolds number airflow transitions
5. Writes the processed data to one or more output formats

Multiple flights are processed in parallel automatically.

## Requirements

- **Rust toolchain**: Version 1.85 or later (2024 edition). Install via [rustup](https://rustup.rs/).
- **PostgreSQL**: A local or remote PostgreSQL database with the PostGIS extension and the `packets` table populated with APRS telemetry data. See `packets-table-reference.sql` for the table schema.
- **Flight list**: A `flightlist.json` file describing the flights to process (included in this repository).

## Installation

Clone the repository and build the release binary:

```bash
git clone <repository-url>
cd historicaleossdata
cargo build --release
```

The compiled binary will be at `target/release/eoss-processor`.

Optionally, copy or symlink it somewhere on your `PATH`:

```bash
cp target/release/eoss-processor /usr/local/bin/
```

## Usage

### Process all flights

```bash
cargo run --release
```

Or, if using the compiled binary directly:

```bash
eoss-processor
```

By default this connects to a local PostgreSQL database named `legacy` via Unix socket and processes every flight in `./flightlist.json`.

### Process a single flight

```bash
eoss-processor --flight EOSS-391
```

Flight names are case-insensitive.

### Produce only a specific output format

```bash
eoss-processor --flight EOSS-391 --output-type parquet
```

Available output types: `csv`, `json`, `parquet`, `xlsx`, `kml`, `all` (default).

### Use a different database

```bash
# By name (Unix socket connection)
eoss-processor --dbname eosstracker

# Remote database
eoss-processor --dbhost db.example.com --dbport 5432 --dbuser myuser --dbpassword mypass
```

### All command-line options

| Option | Default | Description |
|--------|---------|-------------|
| `--dbname` | `legacy` | PostgreSQL database name. Also set via `EOSS_DBNAME` env var. |
| `--dbhost` | *(none)* | Database host. When omitted, connects via Unix socket (no password required). Also set via `PGHOST`. |
| `--dbport` | `5432` | Database port (only used with `--dbhost`). Also set via `PGPORT`. |
| `--dbuser` | *(none)* | Database user (only used with `--dbhost`). Also set via `PGUSER`. |
| `--dbpassword` | *(none)* | Database password (only used with `--dbhost`). Prefer `PGPASSWORD` env var. |
| `--flightlist` | `./flightlist.json` | Path to the flight list JSON file. |
| `--output-dir` | `./output` | Root directory for all output files. |
| `--flight` | *(none)* | Process a single flight by name. If omitted, all flights are processed. |
| `--output-type` | `all` | Output format(s) to produce: `csv`, `json`, `parquet`, `xlsx`, `kml`, or `all`. |

## Flight List

The `flightlist.json` file defines the flights to process. A sample is included in this repository for reference, but the maintained version is hosted on the primary EOSS tracking website at:

**https://track.eoss.org/flightlist.json**

Download the latest version before processing to ensure you have current flight data:

```bash
curl -o flightlist.json https://track.eoss.org/flightlist.json
```

Each entry contains:

```json
{
    "flight": "EOSS-391",
    "beacons": ["KC0D-2", "AE0SS-2", "AE0SS-1"],
    "day": "2026-03-21",
    "balloonsize": "1500gm",
    "parachute": {
        "description": "Rocketman",
        "size": "12"
    },
    "weights": {
        "client": "7.64",
        "eoss": "1.16",
        "parachute": "1.70",
        "neckload": "10.50",
        "balloon": "3.31",
        "gross": "13.80",
        "necklift": "14.64"
    },
    "liftfactor": "1.30",
    "h2fill": "242"
}
```

| Field | Description |
|-------|-------------|
| `flight` | Flight identifier (e.g., `EOSS-391`) |
| `beacons` | APRS callsigns of the beacons carried on this flight |
| `day` | Launch date in `YYYY-MM-DD` format |
| `balloonsize` | Balloon size designation |
| `parachute` | Parachute description and size (in feet) |
| `weights` | Component weights in pounds (client payload, EOSS equipment, parachute, etc.) |
| `liftfactor` | Lift factor used for hydrogen fill calculation |
| `h2fill` | Hydrogen fill volume |

To add a new flight, append an entry to this file following the same structure.

## Output

All output is written to subdirectories under the output directory (default `./output/`).

### Directory Structure

```
output/
  csv/
    eoss-391.csv                  Per-flight packet data
    eoss-391_metadata.csv         Per-flight metadata (single row)
    flights_metadata.csv          All flights metadata combined
  json/
    eoss-391.json                 Per-flight metadata + packet data
    flights_metadata.json         All flights metadata combined
  parquet/
    eoss-391.parquet              Per-flight packet data (columnar binary)
  xlsx/
    eoss-391.xlsx                 Per-flight workbook (Metadata, Ascent, Descent sheets)
    flights_metadata.xlsx         All flights metadata combined
  kml/
    eoss-391.kml                  Google Earth flight visualization
```

All filenames are lowercased versions of the flight name.

### Format Details

**CSV** -- Comma-separated text files. The per-flight CSV contains one row per telemetry packet with all computed columns. Directly importable into spreadsheets or data analysis tools.

**JSON** -- The per-flight JSON file contains flight metadata (burst altitude, flight time, launch/landing coordinates, Reynolds transitions, weights, etc.) at the top level, with a `packets` array containing all telemetry rows. Useful for programmatic consumption.

**Parquet** -- Apache Parquet columnar format. Compact and fast to read. Best suited for analysis with tools like Python/Pandas, Polars, DuckDB, or Apache Spark.

**Excel (XLSX)** -- Multi-sheet workbook. The Metadata sheet has flight-level summary data. The Ascent and Descent sheets contain the full packet data for each phase. DateTime columns are formatted as `mm/dd/yy hh:mm:ss.000`.

**KML** -- Google Earth visualization file. Open with [Google Earth](https://earth.google.com/) to see:
- 3D flight paths (red for ascent, blue for descent)
- Altitude waypoints at 10,000 ft intervals
- Points of interest: Launch, Landing, Burst, and Reynolds transition altitudes
- HTML popup descriptions with timestamps, altitudes, and coordinates

### Key Columns in Output Data

| Column | Units | Description |
|--------|-------|-------------|
| `altitude_ft` / `altitude_m` | ft / m | GPS altitude |
| `velocity_z_fts` / `velocity_z_ms` | ft/s / m/s | Vertical velocity (ascent rate) |
| `vert_rate_ftmin` | ft/min | Vertical rate |
| `acceleration_fts2` / `acceleration_ms2` | ft/s^2 / m/s^2 | Vertical acceleration |
| `airdensity_slugs` / `airdensity_kgm3` | slugs/ft^3 / kg/m^3 | Computed air density |
| `velocity_curvefit_fts` / `velocity_curvefit_ms` | ft/s / m/s | Polynomial curve fit of velocity vs. altitude |
| `distance_from_launch_mi` / `distance_from_launch_km` | mi / km | Great-circle distance from launch point |
| `temperature_f` / `temperature_c` / `temperature_k` | F / C / K | On-board temperature sensor |
| `pressure_pa` / `pressure_atm` | Pa / atm | On-board pressure sensor |
| `airflow` | -- | Reynolds regime label: `high Re`, `low Re`, or `n/a` |
| `flight_phase` | -- | `ascending` or `descending` |

### Consolidated Metadata

When processing multiple flights (or with `--output-type all`), the application generates consolidated metadata files that combine summary data from all processed flights into a single CSV, JSON, and XLSX file. These are written as `flights_metadata.*` in their respective output directories.

## Database Setup

The application requires a PostgreSQL database with the PostGIS extension and a `packets` table. The table schema is defined in `packets-table-reference.sql`:

```bash
# Create the database and enable PostGIS
createdb legacy
psql -d legacy -c "CREATE EXTENSION IF NOT EXISTS postgis;"

# Create the packets table
psql -d legacy -f packets-table-reference.sql
```

The `packets` table must be populated with APRS telemetry data. The application queries packets by callsign and timestamp, filtering to the flight day window.

## Troubleshooting

**"Failed to connect to database 'legacy'"** -- Ensure PostgreSQL is running and the database exists. If connecting locally, the current OS user must have peer authentication access. For remote connections, use `--dbhost`, `--dbuser`, and `--dbpassword` (or their corresponding environment variables).

**"No rows returned for flight EOSS-XXX"** -- The database does not contain APRS packets matching the beacons and date for this flight. Verify the beacon callsigns and date in `flightlist.json` match what is in the database.

**"Too few rows after trimming"** -- The flight had data but nearly all packets were filtered out during pre-launch/post-landing trimming or outlier removal. This can happen if the flight data is sparse or the altitude profile is unusual.

**KML file not generated** -- KML output requires both ascent and descent data. Flights where the descent phase has no valid packets (e.g., beacon lost at burst) will skip KML generation.

## License

This project is licensed under the GNU General Public License v3.0. See [LICENSE](LICENSE) for details.
