use anyhow::Result;
use polars::prelude::*;
use std::fs::File;

use super::reorder_columns;

/// Write a consolidated DataFrame to CSV
pub fn write_csv(df: &DataFrame, path: &str) -> Result<()> {
    let df = reorder_columns(df)?;
    let mut file = File::create(path)?;
    CsvWriter::new(&mut file).finish(&mut df.clone())?;
    Ok(())
}
