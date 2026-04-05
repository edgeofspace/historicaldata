use clap::{Parser, ValueEnum};

#[derive(Debug, Clone, ValueEnum, PartialEq)]
pub enum OutputType {
    Csv,
    Json,
    Parquet,
    Xlsx,
    Kml,
    All,
}

#[derive(Parser, Debug)]
#[command(name = "eoss-processor", about = "Process EOSS high-altitude balloon flight data")]
pub struct Config {
    /// PostgreSQL database name
    #[arg(long, default_value = "legacy", env = "EOSS_DBNAME")]
    pub dbname: String,

    /// PostgreSQL host. If omitted, connects via Unix socket (no password needed).
    #[arg(long, env = "PGHOST")]
    pub dbhost: Option<String>,

    /// PostgreSQL port (only used when --dbhost is set)
    #[arg(long, default_value = "5432", env = "PGPORT")]
    pub dbport: u16,

    /// PostgreSQL password (only used when --dbhost is set). Prefer PGPASSWORD env var.
    #[arg(long, env = "PGPASSWORD", hide_env_values = true)]
    pub dbpassword: Option<String>,

    /// PostgreSQL user (only used when --dbhost is set)
    #[arg(long, env = "PGUSER")]
    pub dbuser: Option<String>,

    /// Path to the flight list JSON file
    #[arg(long, default_value = "./flightlist.json")]
    pub flightlist: String,

    /// Output directory
    #[arg(long, default_value = "./output")]
    pub output_dir: String,

    /// Process a single flight by name (e.g. EOSS-391). If omitted, process all flights.
    #[arg(long)]
    pub flight: Option<String>,

    /// Output format(s) to produce
    #[arg(long, value_enum, default_value = "all")]
    pub output_type: OutputType,
}
