use anyhow::Result;
use polars::prelude::*;
use regex::Regex;

use crate::models::ReynoldsTransition;
use crate::physics;

/// Result of processing a flight's data
pub struct ProcessedFlight {
    pub ascent: DataFrame,
    pub descent: DataFrame,
    pub detected_burst: Option<i64>,
    pub curve_fit_degree: Option<usize>,
    pub reynolds_transitions: Vec<ReynoldsTransition>,
    pub max_altitude_ft: f64,
    pub max_altitude_m: f64,
}

/// Round a float to n decimal places
fn round(val: f64, places: i32) -> f64 {
    let factor = 10f64.powi(places);
    (val * factor).round() / factor
}

/// Core processing pipeline, equivalent to Python's process_df() + processThread()
pub fn process_df(flightname: &str, df: DataFrame) -> Result<ProcessedFlight> {
    // Detect burst packets BEFORE dropping nulls, since burst status packets
    // lack position/altitude data and would be removed by drop_nulls
    let burst_re = Regex::new(r"(?i)DETECTED.*BURST|DETECTED COMMANDED RELEASE").unwrap();
    let alt_re = Regex::new(r"(?i)(\d+)\s*ft").unwrap();
    let detected_burst = {
        let raw_col = df.column("raw")?.str()?;
        let mut detected_altitudes: Vec<i64> = Vec::new();
        for opt_raw in raw_col.into_iter() {
            if let Some(raw) = opt_raw {
                if burst_re.is_match(raw) {
                    if let Some(caps) = alt_re.captures(raw) {
                        if let Ok(alt) = caps[1].parse::<i64>() {
                            detected_altitudes.push(alt);
                        }
                    }
                }
            }
        }
        detected_altitudes.into_iter().max()
    };

    // Drop rows where essential position/time columns are null
    let essential_cols: Vec<String> = vec!["altitude_ft", "latitude", "longitude", "packettime"]
        .into_iter().map(String::from).collect();
    let df = df.drop_nulls(Some(&essential_cols))?;

    let nrows = df.height();
    if nrows == 0 {
        anyhow::bail!("No data rows for flight {flightname}");
    }

    // Add flightid column
    let flightid_col = Column::new("flightid".into(), vec![flightname; nrows]);
    let mut df = df.hstack(&[flightid_col])?;

    // Add position_packet column: altitude_ft > 0 AND altitude_ft not null AND latitude not null AND longitude not null
    let alt_ft = df.column("altitude_ft")?.f64()?;
    let lat = df.column("latitude")?.f64()?;
    let lon = df.column("longitude")?.f64()?;

    let position_packet: BooleanChunked = alt_ft
        .into_iter()
        .zip(lat.into_iter())
        .zip(lon.into_iter())
        .map(|((a, la), lo)| {
            Some(a.is_some_and(|v| v > 0.0) && la.is_some() && lo.is_some())
        })
        .collect();
    let position_packet = position_packet.with_name("position_packet".into());
    df.with_column(position_packet.into_column())?;

    // Get unique callsigns
    let callsigns: Vec<String> = df
        .column("callsign")?
        .str()?
        .into_iter()
        .filter_map(|s| s.map(|s| s.to_string()))
        .collect::<std::collections::HashSet<_>>()
        .into_iter()
        .collect();

    // Process each beacon separately
    let pos_mask = df.column("position_packet")?.bool()?;
    let mut consolidated_frames: Vec<DataFrame> = Vec::new();

    for beacon in &callsigns {
        let cs_col = df.column("callsign")?.str()?;

        // Filter: callsign == beacon AND position_packet == true
        let mask: BooleanChunked = cs_col
            .into_iter()
            .zip(pos_mask.into_iter())
            .map(|(cs, pp)| Some(cs == Some(beacon.as_str()) && pp == Some(true)))
            .collect();

        let mut beacon_df = df.filter(&mask)?;
        if beacon_df.height() == 0 {
            continue;
        }

        // Sort by packettime
        beacon_df = beacon_df.sort(["packettime"], SortMultipleOptions::default())?;

        // Drop duplicate packettimes (keep first)
        let subset: &[String] = &["packettime".to_string()];
        beacon_df = beacon_df.unique::<String, String>(Some(subset), UniqueKeepStrategy::First, None)?;
        beacon_df = beacon_df.sort(["packettime"], SortMultipleOptions::default())?;

        let h = beacon_df.height();
        if h < 2 {
            continue;
        }

        // Compute timedelta_s
        let pt = beacon_df.column("packettime")?.datetime()?.clone();
        let mut timedelta_s: Vec<Option<f64>> = Vec::with_capacity(h);
        timedelta_s.push(None);
        for i in 1..h {
            match (pt.get(i), pt.get(i - 1)) {
                (Some(t1), Some(t0)) => {
                    timedelta_s.push(Some((t1 - t0) as f64 / 1000.0));
                }
                _ => timedelta_s.push(None),
            }
        }

        // Compute velocities
        let alt_ft_arr = beacon_df.column("altitude_ft")?.f64()?.clone();
        let alt_m_arr = beacon_df.column("altitude_m")?.f64()?.clone();
        let lat_arr = beacon_df.column("latitude")?.f64()?.clone();
        let lon_arr = beacon_df.column("longitude")?.f64()?.clone();
        let pres_arr = beacon_df.column("pressure_pa")?.f64()?.clone();
        let temp_arr = beacon_df.column("temperature_k")?.f64()?.clone();

        let mut vel_x: Vec<Option<f64>> = vec![None; h];
        let mut vel_y: Vec<Option<f64>> = vec![None; h];
        let mut vel_z_fts: Vec<Option<f64>> = vec![None; h];
        let mut vel_z_ms: Vec<Option<f64>> = vec![None; h];
        let mut vert_rate: Vec<Option<f64>> = vec![None; h];
        let mut density_slugs: Vec<Option<f64>> = vec![None; h];
        let mut density_kgm3: Vec<Option<f64>> = vec![None; h];

        for i in 1..h {
            if let Some(dt) = timedelta_s[i] {
                if dt > 0.0 {
                    if let (Some(lon1), Some(lon0)) = (lon_arr.get(i), lon_arr.get(i - 1)) {
                        vel_x[i] = Some(round((lon1 - lon0) / dt, 8));
                    }
                    if let (Some(lat1), Some(lat0)) = (lat_arr.get(i), lat_arr.get(i - 1)) {
                        vel_y[i] = Some(round((lat1 - lat0) / dt, 8));
                    }
                    if let (Some(a1), Some(a0)) = (alt_ft_arr.get(i), alt_ft_arr.get(i - 1)) {
                        let vz = round((a1 - a0) / dt, 8);
                        vel_z_fts[i] = Some(vz);
                        vert_rate[i] = Some(round(vz * 60.0, 1));
                    }
                    if let (Some(a1), Some(a0)) = (alt_m_arr.get(i), alt_m_arr.get(i - 1)) {
                        vel_z_ms[i] = Some(round((a1 - a0) / dt, 8));
                    }
                }
            }
            if let (Some(p), Some(t)) = (pres_arr.get(i), temp_arr.get(i)) {
                if t > 0.0 {
                    density_slugs[i] = Some(round(physics::air_density_slugs(p, t), 8));
                    density_kgm3[i] = Some(round(physics::air_density_kgm3(p, t), 8));
                }
            }
        }
        // Also compute density for row 0
        if let (Some(p), Some(t)) = (pres_arr.get(0), temp_arr.get(0)) {
            if t > 0.0 {
                density_slugs[0] = Some(round(physics::air_density_slugs(p, t), 8));
                density_kgm3[0] = Some(round(physics::air_density_kgm3(p, t), 8));
            }
        }

        beacon_df.with_column(Column::new("timedelta_s".into(), timedelta_s))?;
        beacon_df.with_column(Column::new("velocity_x_degs".into(), vel_x))?;
        beacon_df.with_column(Column::new("velocity_y_degs".into(), vel_y))?;
        beacon_df.with_column(Column::new("velocity_z_fts".into(), vel_z_fts))?;
        beacon_df.with_column(Column::new("velocity_z_ms".into(), vel_z_ms))?;
        beacon_df.with_column(Column::new("vert_rate_ftmin".into(), vert_rate))?;
        beacon_df.with_column(Column::new("airdensity_slugs".into(), density_slugs))?;
        beacon_df.with_column(Column::new("airdensity_kgm3".into(), density_kgm3))?;

        consolidated_frames.push(beacon_df);
    }

    if consolidated_frames.is_empty() {
        anyhow::bail!("No position data for flight {flightname}");
    }

    // Concatenate all beacon DataFrames
    let mut consolidated = if consolidated_frames.len() == 1 {
        consolidated_frames.pop().unwrap()
    } else {
        // Ensure all frames have the same columns before concat
        let args = UnionArgs::default();
        concat_df(consolidated_frames.iter().collect::<Vec<_>>().as_slice(), args)?
    };

    // Sort by packettime
    consolidated = consolidated.sort(["packettime"], SortMultipleOptions::default())?;

    let nrows = consolidated.height();
    if nrows < 3 {
        anyhow::bail!("Too few rows ({nrows}) for flight {flightname}");
    }

    // Add default airflow column
    let airflow_default: Vec<&str> = vec!["n/a"; nrows];
    consolidated.with_column(Column::new("airflow".into(), airflow_default))?;

    // Determine max altitude and ascending flag
    let alt_ft = consolidated.column("altitude_ft")?.f64()?;
    let mut max_alt_val = f64::NEG_INFINITY;
    let mut altmax_idx: usize = 0;
    for i in 0..nrows {
        if let Some(v) = alt_ft.get(i) {
            if v > max_alt_val {
                max_alt_val = v;
                altmax_idx = i;
            }
        }
    }

    let max_altitude_ft = max_alt_val;
    let max_altitude_m = consolidated
        .column("altitude_m")?
        .f64()?
        .get(altmax_idx)
        .unwrap_or(0.0);

    let ascending: Vec<bool> = (0..nrows).map(|i| i <= altmax_idx).collect();
    consolidated.with_column(Column::new("ascending".into(), ascending))?;

    // Remove stragglers: timedelta_s > 500 & altitude_ft < 8000 & !ascending
    let td = consolidated.column("timedelta_s")?.f64()?;
    let alt = consolidated.column("altitude_ft")?.f64()?;
    let asc = consolidated.column("ascending")?.bool()?;

    let mut straggler_start: Option<usize> = None;
    for i in 0..nrows {
        let is_straggler = td.get(i).is_some_and(|t| t > 500.0)
            && alt.get(i).is_some_and(|a| a < 8000.0)
            && asc.get(i) == Some(false);
        if is_straggler && straggler_start.is_none() {
            straggler_start = Some(i + 1); // keep up to and including this row
            break;
        }
    }

    if let Some(end) = straggler_start {
        consolidated = consolidated.slice(0, end);
    }

    // Eject outliers
    let vel_z = consolidated.column("velocity_z_fts")?.f64()?;
    let asc = consolidated.column("ascending")?.bool()?;
    let keep_mask: BooleanChunked = vel_z
        .into_iter()
        .zip(asc.into_iter())
        .map(|(vz, a)| {
            match (vz, a) {
                (Some(v), Some(true)) => Some(v < 50.0 && v > -5.0),
                (Some(v), Some(false)) => Some(v < 5.0),
                _ => Some(true), // keep nulls for now
            }
        })
        .collect();
    consolidated = consolidated.filter(&keep_mask)?;

    // Re-sort after filtering
    consolidated = consolidated.sort(["packettime"], SortMultipleOptions::default())?;

    let nrows = consolidated.height();
    if nrows < 3 {
        anyhow::bail!("Too few rows after filtering for flight {flightname}");
    }

    // Compute forward moving averages (reverse -> rolling_mean -> reverse)
    let vel_z_fts = consolidated.column("velocity_z_fts")?.f64()?.clone();
    let vel_vals: Vec<f64> = vel_z_fts
        .into_iter()
        .map(|v| v.unwrap_or(0.0))
        .collect();

    let forward_avg = compute_forward_avg(&vel_vals, 3);
    let long_forward_avg = compute_forward_avg(&vel_vals, 10);

    consolidated.with_column(Column::new("forward_avg".into(), forward_avg.clone()))?;
    consolidated.with_column(Column::new("long_forward_avg".into(), long_forward_avg.clone()))?;

    // Launch detection
    let vel_z = consolidated.column("velocity_z_fts")?.f64()?;
    let fa = consolidated.column("forward_avg")?.f64()?;
    let lfa = consolidated.column("long_forward_avg")?.f64()?;
    let asc = consolidated.column("ascending")?.bool()?;
    let alt = consolidated.column("altitude_ft")?.f64()?;

    let mut starting_idx: usize = 0;
    for i in 0..nrows {
        let vz = vel_z.get(i).unwrap_or(0.0);
        let f = fa.get(i).unwrap_or(0.0);
        let l = lfa.get(i).unwrap_or(0.0);
        let is_asc = asc.get(i).unwrap_or(false);
        let a = alt.get(i).unwrap_or(0.0);

        if vz > 0.0 && f > 5.0 && l > 5.0 && is_asc && a < 8000.0 {
            starting_idx = if i >= 1 { i - 1 } else { 0 };
            break;
        }
    }

    // Landing detection
    let mut ending_idx = nrows - 1;
    for i in 0..nrows {
        let vz = vel_z.get(i).unwrap_or(0.0);
        let f = fa.get(i).unwrap_or(0.0);
        let l = lfa.get(i).unwrap_or(0.0);
        let is_asc = asc.get(i).unwrap_or(false);
        let a = alt.get(i).unwrap_or(0.0);

        if vz > -5.0 && f > -5.0 && l > -5.0 && !is_asc && a < 8000.0 && i > starting_idx {
            ending_idx = (i + 1).min(nrows - 1);
            break;
        }
    }

    if starting_idx > 0 || ending_idx < nrows - 1 {
        let len = ending_idx - starting_idx;
        consolidated = consolidated.slice(starting_idx as i64, len);
    }

    let nrows = consolidated.height();
    if nrows < 3 {
        anyhow::bail!("Too few rows after trimming for flight {flightname}");
    }

    // Distance from launch
    let lat_arr = consolidated.column("latitude")?.f64()?.clone();
    let lon_arr = consolidated.column("longitude")?.f64()?.clone();
    let first_lat = lat_arr.get(0).unwrap_or(0.0);
    let first_lon = lon_arr.get(0).unwrap_or(0.0);

    let dist_mi: Vec<Option<f64>> = (0..nrows)
        .map(|i| {
            match (lat_arr.get(i), lon_arr.get(i)) {
                (Some(la), Some(lo)) => {
                    Some(round(physics::haversine_distance(first_lat, first_lon, la, lo), 2))
                }
                _ => None,
            }
        })
        .collect();
    let dist_km: Vec<Option<f64>> = dist_mi
        .iter()
        .map(|d| d.map(|v| round(v * 1.609344, 2)))
        .collect();

    consolidated.with_column(Column::new("distance_from_launch_mi".into(), dist_mi))?;
    consolidated.with_column(Column::new("distance_from_launch_km".into(), dist_km))?;

    // Re-determine ascending flag after trimming
    let alt_ft = consolidated.column("altitude_ft")?.f64()?;
    let mut max_alt_val2 = f64::NEG_INFINITY;
    let mut altmax_idx2: usize = 0;
    for i in 0..nrows {
        if let Some(v) = alt_ft.get(i) {
            if v > max_alt_val2 {
                max_alt_val2 = v;
                altmax_idx2 = i;
            }
        }
    }

    let ascending2: Vec<bool> = (0..nrows).map(|i| i <= altmax_idx2).collect();
    consolidated.with_column(Column::new("ascending".into(), ascending2))?;

    // Split into ascent and descent
    let asc_col = consolidated.column("ascending")?.bool()?;
    let asc_mask: BooleanChunked = asc_col.into_iter().map(|v| v).collect();
    let desc_mask: BooleanChunked = asc_col.into_iter().map(|v| v.map(|b| !b)).collect();

    let mut ascent = consolidated.filter(&asc_mask)?;
    let mut descent = consolidated.filter(&desc_mask)?;

    let mut curve_fit_degree: Option<usize> = None;
    let mut reynolds_transitions: Vec<ReynoldsTransition> = Vec::new();

    // Process ascent phase
    if ascent.height() > 2 {
        ascent = process_phase(&mut ascent, true)?;

        // Reynolds transition detection
        let dtl = ascent.column("distance_to_line")?.f64()?;
        let alt = ascent.column("altitude_ft")?.f64()?;

        let minimum_distance = 30.0;
        let altitude_min = 10000.0;

        let mut max_dist_idx: Option<usize> = None;
        let mut min_dist_idx: Option<usize> = None;
        let mut max_dist = 0.0f64;
        let mut min_dist = 0.0f64;

        for i in 0..ascent.height() {
            let d = dtl.get(i).unwrap_or(0.0);
            let a = alt.get(i).unwrap_or(0.0);
            if a > altitude_min {
                if d > minimum_distance && d > max_dist {
                    max_dist = d;
                    max_dist_idx = Some(i);
                }
                if d < -minimum_distance && d < min_dist {
                    min_dist = d;
                    min_dist_idx = Some(i);
                }
            }
        }

        // Assign airflow labels
        let n = ascent.height();
        let mut airflow_labels: Vec<String> = vec!["n/a".to_string(); n];

        if max_dist > 0.0 && min_dist < 0.0 {
            let mx = max_dist_idx.unwrap();
            let mn = min_dist_idx.unwrap();
            if mx > mn {
                for i in 0..=mn { airflow_labels[i] = "high Re".to_string(); }
                for i in mn..=mx { airflow_labels[i] = "low Re".to_string(); }
                for i in mx..n { airflow_labels[i] = "high Re".to_string(); }
            } else {
                for i in 0..=mx { airflow_labels[i] = "low Re".to_string(); }
                for i in mx..=mn { airflow_labels[i] = "high Re".to_string(); }
                for i in mn..n { airflow_labels[i] = "low Re".to_string(); }
            }
        } else if max_dist > 0.0 {
            let mx = max_dist_idx.unwrap();
            for i in 0..=mx { airflow_labels[i] = "low Re".to_string(); }
            for i in mx..n { airflow_labels[i] = "high Re".to_string(); }
        } else if min_dist < 0.0 {
            let mn = min_dist_idx.unwrap();
            for i in 0..=mn { airflow_labels[i] = "high Re".to_string(); }
            for i in mn..n { airflow_labels[i] = "low Re".to_string(); }
        }

        ascent.with_column(Column::new("airflow".into(), airflow_labels))?;

        // Get polynomial degree (we need it for metadata)
        let vz: Vec<f64> = ascent.column("velocity_z_fts")?.f64()?.into_iter().filter_map(|v| v).collect();
        if !vz.is_empty() {
            let mean_vz: f64 = vz.iter().sum::<f64>() / vz.len() as f64;
            let var_vz: f64 = vz.iter().map(|v| (v - mean_vz).powi(2)).sum::<f64>() / (vz.len() as f64 - 1.0);
            let vmr = if mean_vz.abs() > 1e-10 { var_vz / mean_vz } else { 0.0 };
            curve_fit_degree = Some(physics::vmr_degree(vmr, 13));
        }

        // Polynomial curve fit for ascent
        let alt_vals: Vec<f64> = ascent.column("altitude_ft")?.f64()?.into_iter().filter_map(|v| v).collect();
        let vel_vals: Vec<f64> = ascent.column("velocity_z_fts")?.f64()?.into_iter().filter_map(|v| v).collect();
        let alt_m_vals: Vec<f64> = ascent.column("altitude_m")?.f64()?.into_iter().filter_map(|v| v).collect();
        let vel_ms_vals: Vec<f64> = ascent.column("velocity_z_ms")?.f64()?.into_iter().filter_map(|v| v).collect();

        if alt_vals.len() > 2 && alt_vals.len() == vel_vals.len() {
            let deg = curve_fit_degree.unwrap_or(5);
            let fit = physics::polynomial_fit(&alt_vals, &vel_vals, deg.min(alt_vals.len() - 1));
            let curvefit: Vec<Option<f64>> = ascent
                .column("altitude_ft")?
                .f64()?
                .into_iter()
                .map(|v| v.map(|x| round(fit(x), 6)))
                .collect();
            ascent.with_column(Column::new("velocity_curvefit_fts".into(), curvefit))?;

            // Metric curve fit
            let vz_ms: Vec<f64> = ascent.column("velocity_z_ms")?.f64()?.into_iter().filter_map(|v| v).collect();
            let mean_ms: f64 = vz_ms.iter().sum::<f64>() / vz_ms.len().max(1) as f64;
            let var_ms: f64 = vz_ms.iter().map(|v| (v - mean_ms).powi(2)).sum::<f64>() / (vz_ms.len() as f64 - 1.0).max(1.0);
            let vmr_m = if mean_ms.abs() > 1e-10 { var_ms / mean_ms } else { 0.0 };
            let deg_m = physics::vmr_degree(vmr_m, 13);

            if alt_m_vals.len() > 2 && alt_m_vals.len() == vel_ms_vals.len() {
                let fit_m = physics::polynomial_fit(&alt_m_vals, &vel_ms_vals, deg_m.min(alt_m_vals.len() - 1));
                let curvefit_m: Vec<Option<f64>> = ascent
                    .column("altitude_m")?
                    .f64()?
                    .into_iter()
                    .map(|v| v.map(|x| round(fit_m(x), 6)))
                    .collect();
                ascent.with_column(Column::new("velocity_curvefit_ms".into(), curvefit_m))?;
            }
        }

        // Build Reynolds transitions from airflow column shifts
        let airflow = ascent.column("airflow")?.str()?;
        let alt_ft = ascent.column("altitude_ft")?.f64()?;
        let alt_m = ascent.column("altitude_m")?.f64()?;

        let mut prev_af: Option<&str> = None;
        for i in 0..ascent.height() {
            let af = airflow.get(i).unwrap_or("n/a");
            if let Some(prev) = prev_af {
                if af != prev && af != "n/a" && prev != "n/a" {
                    let prior_re = prev.split(' ').next().unwrap_or("");
                    let next_re = af.split(' ').next().unwrap_or("");
                    let transition = format!("{prior_re}_to_{next_re}");
                    reynolds_transitions.push(ReynoldsTransition {
                        transition,
                        altitude_ft: alt_ft.get(i).unwrap_or(0.0),
                        altitude_m: alt_m.get(i).unwrap_or(0.0),
                    });
                }
            }
            prev_af = Some(af);
        }

        // Drop temp columns from ascent
        let _ = ascent.drop_in_place("distance_to_line");
        let _ = ascent.drop_in_place("forward_avg");
        let _ = ascent.drop_in_place("long_forward_avg");
        let _ = ascent.drop_in_place("timedelta_s");
    } else {
        eprintln!("ERROR: {} {:?} ascent shape was: {}", flightname.to_lowercase(), callsigns, ascent.height());
        anyhow::bail!("Insufficient ascent data for flight {flightname}");
    }

    // Process descent phase
    if descent.height() > 2 {
        descent = process_phase(&mut descent, false)?;

        // Polynomial curve fit for descent
        let alt_vals: Vec<f64> = descent.column("altitude_ft")?.f64()?.into_iter().filter_map(|v| v).collect();
        let vel_vals: Vec<f64> = descent.column("velocity_z_fts")?.f64()?.into_iter().filter_map(|v| v).collect();
        let alt_m_vals: Vec<f64> = descent.column("altitude_m")?.f64()?.into_iter().filter_map(|v| v).collect();
        let vel_ms_vals: Vec<f64> = descent.column("velocity_z_ms")?.f64()?.into_iter().filter_map(|v| v).collect();

        if alt_vals.len() > 2 && alt_vals.len() == vel_vals.len() {
            let mean_vz: f64 = vel_vals.iter().sum::<f64>() / vel_vals.len() as f64;
            let var_vz: f64 = vel_vals.iter().map(|v| (v - mean_vz).powi(2)).sum::<f64>() / (vel_vals.len() as f64 - 1.0);
            let vmr = if mean_vz.abs() > 1e-10 { var_vz / mean_vz } else { 0.0 };
            let deg = physics::vmr_degree(vmr, 13);
            let fit = physics::polynomial_fit(&alt_vals, &vel_vals, deg.min(alt_vals.len() - 1));
            let curvefit: Vec<Option<f64>> = descent
                .column("altitude_ft")?
                .f64()?
                .into_iter()
                .map(|v| v.map(|x| round(fit(x), 6)))
                .collect();
            descent.with_column(Column::new("velocity_curvefit_fts".into(), curvefit))?;

            // Metric
            let mean_ms: f64 = vel_ms_vals.iter().sum::<f64>() / vel_ms_vals.len().max(1) as f64;
            let var_ms: f64 = vel_ms_vals.iter().map(|v| (v - mean_ms).powi(2)).sum::<f64>() / (vel_ms_vals.len() as f64 - 1.0).max(1.0);
            let vmr_m = if mean_ms.abs() > 1e-10 { var_ms / mean_ms } else { 0.0 };
            let deg_m = physics::vmr_degree(vmr_m, 13);

            if alt_m_vals.len() > 2 && alt_m_vals.len() == vel_ms_vals.len() {
                let fit_m = physics::polynomial_fit(&alt_m_vals, &vel_ms_vals, deg_m.min(alt_m_vals.len() - 1));
                let curvefit_m: Vec<Option<f64>> = descent
                    .column("altitude_m")?
                    .f64()?
                    .into_iter()
                    .map(|v| v.map(|x| round(fit_m(x), 6)))
                    .collect();
                descent.with_column(Column::new("velocity_curvefit_ms".into(), curvefit_m))?;
            }
        }

        // Drop temp columns from descent
        let _ = descent.drop_in_place("distance_to_line");
        let _ = descent.drop_in_place("forward_avg");
        let _ = descent.drop_in_place("long_forward_avg");
        let _ = descent.drop_in_place("timedelta_s");
    } else {
        eprintln!("ERROR: {} {:?} descent shape was: {}", flightname.to_lowercase(), callsigns, descent.height());
    }

    // Add flight_phase column
    let asc_n = ascent.height();
    let phase_asc: Vec<&str> = vec!["ascending"; asc_n];
    ascent.with_column(Column::new("flight_phase".into(), phase_asc))?;

    let desc_n = descent.height();
    let phase_desc: Vec<&str> = vec!["descending"; desc_n];
    descent.with_column(Column::new("flight_phase".into(), phase_desc))?;

    // Add reynolds_transition column to ascent
    let airflow = ascent.column("airflow")?.str()?.clone();
    let mut re_trans_col: Vec<String> = vec![String::new(); asc_n];
    let mut prev_af: Option<String> = None;
    for i in 0..asc_n {
        let af = airflow.get(i).unwrap_or("n/a").to_string();
        if let Some(ref prev) = prev_af {
            if af != *prev && af != "n/a" && prev != "n/a" {
                let prior_re = prev.split(' ').next().unwrap_or("");
                let next_re = af.split(' ').next().unwrap_or("");
                re_trans_col[i] = format!("{prior_re}_to_{next_re}");
            }
        }
        prev_af = Some(af);
    }
    ascent.with_column(Column::new("reynolds_transition".into(), re_trans_col))?;

    // Empty reynolds_transition for descent
    let re_trans_desc: Vec<&str> = vec![""; desc_n];
    descent.with_column(Column::new("reynolds_transition".into(), re_trans_desc))?;

    // Drop 'ascending' and 'index' columns if present
    let _ = ascent.drop_in_place("ascending");
    let _ = descent.drop_in_place("ascending");
    let _ = ascent.drop_in_place("index");
    let _ = descent.drop_in_place("index");

    // Print stats
    println!(
        "{} {:?}   final rows: {}, ascent: {}, descent: {}, max_alt: {}",
        flightname.to_lowercase(),
        callsigns,
        nrows,
        ascent.height(),
        descent.height(),
        max_altitude_ft,
    );

    Ok(ProcessedFlight {
        ascent,
        descent,
        detected_burst,
        curve_fit_degree,
        reynolds_transitions,
        max_altitude_ft,
        max_altitude_m,
    })
}

/// Process a single phase (ascent or descent) adding computed columns
fn process_phase(df: &mut DataFrame, _is_ascent: bool) -> Result<DataFrame> {
    let h = df.height();

    // Compute elapsed_secs from phase start
    let pt = df.column("packettime")?.datetime()?;
    let start_ms = pt.get(0).unwrap_or(0);
    let elapsed: Vec<Option<f64>> = (0..h)
        .map(|i| pt.get(i).map(|t| (t - start_ms) as f64 / 1000.0))
        .collect();
    df.with_column(Column::new("elapsed_secs".into(), elapsed))?;

    // Line equation: y = mx + b from first to last point
    let elapsed = df.column("elapsed_secs")?.f64()?;
    let alt_ft = df.column("altitude_ft")?.f64()?;
    let t0 = elapsed.get(0).unwrap_or(0.0);
    let t1 = elapsed.get(h - 1).unwrap_or(1.0);
    let a0 = alt_ft.get(0).unwrap_or(0.0);
    let a1 = alt_ft.get(h - 1).unwrap_or(0.0);
    let dt = t1 - t0;
    let m = if dt.abs() > 1e-10 { (a1 - a0) / dt } else { 0.0 };
    let b = a0 - m * t0;

    // Distance to line
    let dtl: Vec<Option<f64>> = (0..h)
        .map(|i| {
            match (elapsed.get(i), alt_ft.get(i)) {
                (Some(x), Some(y)) => Some(round(physics::distance_to_line(x, y, m, b), 6)),
                _ => None,
            }
        })
        .collect();
    df.with_column(Column::new("distance_to_line".into(), dtl))?;

    // Acceleration
    let vel_fts = df.column("velocity_z_fts")?.f64()?.clone();
    let vel_ms = df.column("velocity_z_ms")?.f64()?.clone();
    let td = df.column("timedelta_s")?.f64()?.clone();

    let mut accel_fts: Vec<Option<f64>> = vec![None; h];
    let mut accel_ms: Vec<Option<f64>> = vec![None; h];
    for i in 1..h {
        if let (Some(v1), Some(v0), Some(dt)) = (vel_fts.get(i), vel_fts.get(i - 1), td.get(i)) {
            if dt > 0.0 {
                accel_fts[i] = Some(round((v1 - v0) / dt, 6));
            }
        }
        if let (Some(v1), Some(v0), Some(dt)) = (vel_ms.get(i), vel_ms.get(i - 1), td.get(i)) {
            if dt > 0.0 {
                accel_ms[i] = Some(round((v1 - v0) / dt, 6));
            }
        }
    }
    df.with_column(Column::new("acceleration_fts2".into(), accel_fts))?;
    df.with_column(Column::new("acceleration_ms2".into(), accel_ms))?;

    // Expanding mean and std for velocity and acceleration
    let vel_fts = df.column("velocity_z_fts")?.f64()?;
    let vel_ms = df.column("velocity_z_ms")?.f64()?;
    let acc_fts = df.column("acceleration_fts2")?.f64()?;
    let acc_ms = df.column("acceleration_ms2")?.f64()?;

    fn expanding_mean_std(col: &Float64Chunked) -> (Vec<Option<f64>>, Vec<Option<f64>>) {
        let n = col.len();
        let mut means = vec![None; n];
        let mut stds = vec![None; n];
        let mut sum = 0.0;
        let mut sum_sq = 0.0;
        let mut count = 0usize;

        for i in 0..n {
            if let Some(v) = col.get(i) {
                sum += v;
                sum_sq += v * v;
                count += 1;
                let mean = sum / count as f64;
                means[i] = Some(round(mean, 6));
                if count > 1 {
                    // Unbiased variance: sum((x - mean)^2) / (n-1)
                    let var = (sum_sq - sum * sum / count as f64) / (count as f64 - 1.0);
                    stds[i] = Some(round(var.max(0.0).sqrt(), 6));
                } else {
                    stds[i] = None; // pandas returns NaN for std of single element
                }
            }
        }
        (means, stds)
    }

    let (vm_fts, vs_fts) = expanding_mean_std(vel_fts);
    let (am_fts, as_fts) = expanding_mean_std(acc_fts);
    let (vm_ms, vs_ms) = expanding_mean_std(vel_ms);
    let (am_ms, as_ms) = expanding_mean_std(acc_ms);

    df.with_column(Column::new("velocity_mean_fts".into(), vm_fts.clone()))?;
    df.with_column(Column::new("velocity_std_fts".into(), vs_fts.clone()))?;
    df.with_column(Column::new("acceleration_mean_fts2".into(), am_fts.clone()))?;
    df.with_column(Column::new("acceleration_std_fts2".into(), as_fts.clone()))?;
    df.with_column(Column::new("velocity_mean_ms".into(), vm_ms.clone()))?;
    df.with_column(Column::new("velocity_std_ms".into(), vs_ms.clone()))?;
    df.with_column(Column::new("acceleration_mean_ms2".into(), am_ms.clone()))?;
    df.with_column(Column::new("acceleration_std_ms2".into(), as_ms.clone()))?;

    // Normalized velocity and acceleration
    fn normalize(vals: &Float64Chunked, means: &[Option<f64>], stds: &[Option<f64>]) -> Vec<Option<f64>> {
        let n = vals.len();
        let mut normed = vec![None; n];
        for i in 0..n {
            if let (Some(v), Some(m), Some(s)) = (vals.get(i), means[i], stds[i]) {
                if s > 0.0 {
                    normed[i] = Some(round((v - m) / s, 6));
                }
            }
        }
        normed
    }

    let vel_fts = df.column("velocity_z_fts")?.f64()?;
    let vel_ms = df.column("velocity_z_ms")?.f64()?;
    let acc_fts = df.column("acceleration_fts2")?.f64()?;
    let acc_ms = df.column("acceleration_ms2")?.f64()?;

    let vn_fts = normalize(vel_fts, &vm_fts, &vs_fts);
    let an_fts = normalize(acc_fts, &am_fts, &as_fts);
    let vn_ms = normalize(vel_ms, &vm_ms, &vs_ms);
    let an_ms = normalize(acc_ms, &am_ms, &as_ms);

    df.with_column(Column::new("velocity_norm_fts".into(), vn_fts))?;
    df.with_column(Column::new("acceleration_norm_fts2".into(), an_fts))?;
    df.with_column(Column::new("velocity_norm_ms".into(), vn_ms))?;
    df.with_column(Column::new("acceleration_norm_ms2".into(), an_ms))?;

    Ok(df.clone())
}

/// Compute forward moving average: reverse, rolling mean, reverse
fn compute_forward_avg(vals: &[f64], window: usize) -> Vec<Option<f64>> {
    let n = vals.len();
    if n == 0 {
        return vec![];
    }

    // Reverse
    let reversed: Vec<f64> = vals.iter().rev().cloned().collect();

    // Rolling mean on reversed
    let mut rolling: Vec<Option<f64>> = vec![None; n];
    let mut sum = 0.0;
    for i in 0..n {
        sum += reversed[i];
        if i >= window {
            sum -= reversed[i - window];
        }
        if i >= window - 1 {
            rolling[i] = Some(round(sum / window as f64, 2));
        }
    }

    // Reverse result back
    rolling.reverse();
    rolling
}

/// Helper to concat DataFrames with potentially different column sets
fn concat_df(frames: &[&DataFrame], _args: UnionArgs) -> Result<DataFrame> {
    if frames.is_empty() {
        anyhow::bail!("No frames to concat");
    }
    if frames.len() == 1 {
        return Ok(frames[0].clone());
    }

    // Get union of all column names, preserving order from first frame
    let mut all_cols: Vec<PlSmallStr> = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for frame in frames {
        for name in frame.get_column_names() {
            if seen.insert(name.clone()) {
                all_cols.push(name.clone());
            }
        }
    }

    // Align all frames to have same columns
    let mut aligned: Vec<DataFrame> = Vec::new();
    for frame in frames {
        let mut cols: Vec<Column> = Vec::new();
        for name in &all_cols {
            if let Ok(col) = frame.column(name) {
                cols.push(col.clone());
            } else {
                // Add null column of appropriate type
                let null_col = Column::new(name.clone(), vec![Option::<f64>::None; frame.height()]);
                cols.push(null_col);
            }
        }
        aligned.push(DataFrame::new(cols)?);
    }

    let mut result = aligned[0].clone();
    for frame in &aligned[1..] {
        result = result.vstack(frame)?;
    }

    Ok(result)
}
