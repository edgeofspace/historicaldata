use anyhow::{Context, Result};
use chrono::NaiveDateTime;
use chrono_tz::America::Denver;
use chrono_tz::Tz;
use polars::prelude::*;
use postgres::{Client, NoTls};

use crate::config::Config;
use crate::models::FlightInput;

/// Determine if a date string (YYYY-MM-DD) falls within DST for America/Denver
pub fn is_dst(date_str: &str) -> bool {
    let dt_str = format!("{date_str} 03:00:00");
    let naive = NaiveDateTime::parse_from_str(&dt_str, "%Y-%m-%d %H:%M:%S")
        .expect("Failed to parse date");
    let tz: Tz = Denver;
    // Use the earliest unambiguous mapping
    let local = naive.and_local_timezone(tz);
    match local {
        chrono::offset::LocalResult::Single(dt) => {
            use chrono::Offset;
            // Denver standard offset is -7h. DST offset is -6h.
            dt.offset().fix().local_minus_utc() == -6 * 3600
        }
        chrono::offset::LocalResult::Ambiguous(dt, _) => {
            use chrono::Offset;
            dt.offset().fix().local_minus_utc() == -6 * 3600
        }
        chrono::offset::LocalResult::None => false,
    }
}

/// The SQL query ported verbatim from Python
const QUERY_SQL: &str = r#"
    select distinct on (1)
        substring(a.raw from position(':' in a.raw)+1) as info,
        date_trunc('milliseconds', a.tm)::timestamp without time zone as receivetime,
        case
            when a.raw similar to '%[0-9]{6}h%' then
                date_trunc('milliseconds', ((to_timestamp(a.tm::date || ' ' || substring(a.raw from position('h' in a.raw) - 6 for 6), 'YYYY-MM-DD HH24MISS')::timestamp at time zone $1) at time zone $1)::timestamp)::timestamp without time zone
            else
                date_trunc('milliseconds', a.tm)::timestamp without time zone
        end as packettime,
        a.callsign,
        a.raw,
        round(a.bearing::numeric, 1)::float8 as bearing,
        round(a.speed_mph::numeric, 1)::float8 as speed_mph,
        round(a.speed_mph::numeric * 1.609344, 1)::float8 as speed_kph,
        round(a.altitude::numeric, 1)::float8 as altitude_ft,
        round(a.altitude::numeric * 0.3048, 2)::float8 as altitude_m,
        cast(st_y(a.location2d) as float8) as latitude,
        cast(st_x(a.location2d) as float8) as longitude,
        case when a.raw similar to '% [-]{0,1}[0-9]{1,6}T[-]{0,1}[0-9]{1,6}P%' then
            round(32.0 + 1.8 * cast(substring(substring(substring(a.raw from ' [-]{0,1}[0-9]{1,6}T[-]{0,1}[0-9]{1,6}P') from ' [-]{0,1}[0-9]{1,6}T') from ' [-]{0,1}[0-9]{1,6}') as decimal) / 10.0, 2)::float8
        else
            NULL::float8
        end as temperature_f,
        case when a.raw similar to '% [-]{0,1}[0-9]{1,6}T[-]{0,1}[0-9]{1,6}P%' then
            round(cast(substring(substring(substring(a.raw from ' [-]{0,1}[0-9]{1,6}T[-]{0,1}[0-9]{1,6}P') from ' [-]{0,1}[0-9]{1,6}T') from ' [-]{0,1}[0-9]{1,6}') as decimal) / 10.0, 2)::float8
        else
            NULL::float8
        end as temperature_c,
        case when a.raw similar to '% [-]{0,1}[0-9]{1,6}T[-]{0,1}[0-9]{1,6}P%' then
            round(273.15 + cast(substring(substring(substring(a.raw from ' [-]{0,1}[0-9]{1,6}T[-]{0,1}[0-9]{1,6}P') from ' [-]{0,1}[0-9]{1,6}T') from ' [-]{0,1}[0-9]{1,6}') as decimal) / 10.0, 2)::float8
        else
            NULL::float8
        end as temperature_k,
        case
            when a.raw similar to '% [-]{0,1}[0-9]{1,6}T[-]{0,1}[0-9]{1,6}P%' then
                round(cast(substring(substring(a.raw from '[0-9]{1,6}P') from '[0-9]{1,6}') as decimal) / 10132.5, 2)::float8
            else
                NULL::float8
        end as pressure_atm,
        case
            when a.raw similar to '% [-]{0,1}[0-9]{1,6}T[-]{0,1}[0-9]{1,6}P%' then
                round(cast(substring(substring(a.raw from '[0-9]{1,6}P') from '[0-9]{1,6}') as decimal) * 10.0, 2)::float8
            else
                NULL::float8
        end as pressure_pa

    from
        packets a

    where
        a.tm > $2 and a.tm < $3
        and a.callsign = ANY($4)
        and a.raw not like '%WA0GEH-10%'

    order by
        1, 2
"#;

/// Build a libpq-style connection string from config.
/// Without --dbhost, connects via Unix socket (peer auth, no password needed).
/// With --dbhost, connects via TCP and includes password/user/port if provided.
pub fn build_connection_string(config: &Config) -> String {
    let mut parts = vec![format!("dbname={}", config.dbname)];

    if let Some(ref host) = config.dbhost {
        parts.push(format!("host={host}"));
        parts.push(format!("port={}", config.dbport));
        if let Some(ref user) = config.dbuser {
            parts.push(format!("user={user}"));
        }
        if let Some(ref password) = config.dbpassword {
            parts.push(format!("password={password}"));
        }
    }

    parts.join(" ")
}

/// Query the database for packets from a specific flight, returning a Polars DataFrame
pub fn query_database(config: &Config, flight: &FlightInput) -> Result<DataFrame> {
    let conn_str = build_connection_string(config);
    let mut client = Client::connect(&conn_str, NoTls)
        .with_context(|| format!("Failed to connect to database '{}'", config.dbname))?;

    let launch_date = &flight.day;
    let timezone = if is_dst(launch_date) { "MDT" } else { "MST" };
    // a.tm is timestamptz and the DB timezone is America/Denver.
    // Python passes plain strings which PG interprets in the server's local timezone.
    // We must localize to Denver time first, then convert to UTC for DateTime<Utc>.
    let start_naive = NaiveDateTime::parse_from_str(
        &format!("{launch_date} 03:00:00"),
        "%Y-%m-%d %H:%M:%S",
    ).with_context(|| format!("Failed to parse start date for {}", flight.flight))?;
    let end_naive = NaiveDateTime::parse_from_str(
        &format!("{launch_date} 23:59:59"),
        "%Y-%m-%d %H:%M:%S",
    ).with_context(|| format!("Failed to parse end date for {}", flight.flight))?;

    let tz: Tz = Denver;
    let start_date = start_naive.and_local_timezone(tz).earliest()
        .with_context(|| format!("Failed to localize start date for {}", flight.flight))?
        .with_timezone(&chrono::Utc);
    let end_date = end_naive.and_local_timezone(tz).earliest()
        .with_context(|| format!("Failed to localize end date for {}", flight.flight))?
        .with_timezone(&chrono::Utc);
    let beacons: Vec<&str> = flight.beacons.iter().map(|s| s.as_str()).collect();

    let rows = client
        .query(QUERY_SQL, &[&timezone, &start_date, &end_date, &beacons])
        .with_context(|| format!("SQL query failed for flight {}", flight.flight))?;

    if rows.is_empty() {
        anyhow::bail!("No rows returned for flight {}", flight.flight);
    }

    // Build column vectors
    let mut info_vec: Vec<Option<String>> = Vec::with_capacity(rows.len());
    let mut receivetime_vec: Vec<Option<i64>> = Vec::with_capacity(rows.len());
    let mut packettime_vec: Vec<Option<i64>> = Vec::with_capacity(rows.len());
    let mut callsign_vec: Vec<Option<String>> = Vec::with_capacity(rows.len());
    let mut raw_vec: Vec<Option<String>> = Vec::with_capacity(rows.len());
    let mut bearing_vec: Vec<Option<f64>> = Vec::with_capacity(rows.len());
    let mut speed_mph_vec: Vec<Option<f64>> = Vec::with_capacity(rows.len());
    let mut speed_kph_vec: Vec<Option<f64>> = Vec::with_capacity(rows.len());
    let mut altitude_ft_vec: Vec<Option<f64>> = Vec::with_capacity(rows.len());
    let mut altitude_m_vec: Vec<Option<f64>> = Vec::with_capacity(rows.len());
    let mut latitude_vec: Vec<Option<f64>> = Vec::with_capacity(rows.len());
    let mut longitude_vec: Vec<Option<f64>> = Vec::with_capacity(rows.len());
    let mut temperature_f_vec: Vec<Option<f64>> = Vec::with_capacity(rows.len());
    let mut temperature_c_vec: Vec<Option<f64>> = Vec::with_capacity(rows.len());
    let mut temperature_k_vec: Vec<Option<f64>> = Vec::with_capacity(rows.len());
    let mut pressure_atm_vec: Vec<Option<f64>> = Vec::with_capacity(rows.len());
    let mut pressure_pa_vec: Vec<Option<f64>> = Vec::with_capacity(rows.len());

    for row in &rows {
        info_vec.push(row.get::<_, Option<String>>("info"));

        let rt: Option<NaiveDateTime> = row.get("receivetime");
        receivetime_vec.push(rt.map(|t| t.and_utc().timestamp_millis()));

        let pt: Option<NaiveDateTime> = row.get("packettime");
        packettime_vec.push(pt.map(|t| t.and_utc().timestamp_millis()));

        callsign_vec.push(row.get::<_, Option<String>>("callsign"));
        raw_vec.push(row.get::<_, Option<String>>("raw"));

        // Numeric columns now come back as float8 (f64) thanks to SQL casts
        bearing_vec.push(row.get::<_, Option<f64>>("bearing"));
        speed_mph_vec.push(row.get::<_, Option<f64>>("speed_mph"));
        speed_kph_vec.push(row.get::<_, Option<f64>>("speed_kph"));
        altitude_ft_vec.push(row.get::<_, Option<f64>>("altitude_ft"));
        altitude_m_vec.push(row.get::<_, Option<f64>>("altitude_m"));
        latitude_vec.push(row.get::<_, Option<f64>>("latitude"));
        longitude_vec.push(row.get::<_, Option<f64>>("longitude"));
        temperature_f_vec.push(row.get::<_, Option<f64>>("temperature_f"));
        temperature_c_vec.push(row.get::<_, Option<f64>>("temperature_c"));
        temperature_k_vec.push(row.get::<_, Option<f64>>("temperature_k"));
        pressure_atm_vec.push(row.get::<_, Option<f64>>("pressure_atm"));
        pressure_pa_vec.push(row.get::<_, Option<f64>>("pressure_pa"));
    }

    // Build datetime columns
    let mut receivetime_ca: Int64Chunked = receivetime_vec.into_iter().collect();
    receivetime_ca.rename("receivetime".into());
    let receivetime_col = receivetime_ca.into_datetime(TimeUnit::Milliseconds, None).into_column();

    let mut packettime_ca: Int64Chunked = packettime_vec.into_iter().collect();
    packettime_ca.rename("packettime".into());
    let packettime_col = packettime_ca.into_datetime(TimeUnit::Milliseconds, None).into_column();

    let df = DataFrame::new(vec![
        Column::new("info".into(), info_vec),
        receivetime_col,
        packettime_col,
        Column::new("callsign".into(), callsign_vec),
        Column::new("raw".into(), raw_vec),
        Column::new("bearing".into(), bearing_vec),
        Column::new("speed_mph".into(), speed_mph_vec),
        Column::new("speed_kph".into(), speed_kph_vec),
        Column::new("altitude_ft".into(), altitude_ft_vec),
        Column::new("altitude_m".into(), altitude_m_vec),
        Column::new("latitude".into(), latitude_vec),
        Column::new("longitude".into(), longitude_vec),
        Column::new("temperature_f".into(), temperature_f_vec),
        Column::new("temperature_c".into(), temperature_c_vec),
        Column::new("temperature_k".into(), temperature_k_vec),
        Column::new("pressure_atm".into(), pressure_atm_vec),
        Column::new("pressure_pa".into(), pressure_pa_vec),
    ])?;

    Ok(df)
}
