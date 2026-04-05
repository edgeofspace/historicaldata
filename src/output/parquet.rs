use anyhow::Result;
use polars::prelude::*;
use std::fs::File;

use super::reorder_columns;

/// Write DataFrame to Parquet format (replaces pickle)
pub fn write_parquet(df: &DataFrame, path: &str) -> Result<()> {
    let df = reorder_columns(df)?;
    let mut file = File::create(path)?;
    ParquetWriter::new(&mut file).finish(&mut df.clone())?;
    Ok(())
}
