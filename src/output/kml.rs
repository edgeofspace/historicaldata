use anyhow::Result;
use polars::prelude::*;
use quick_xml::events::{BytesCData, BytesDecl, BytesEnd, BytesStart, BytesText, Event};
use quick_xml::Writer;
use std::fs::File;
use std::io::BufWriter;

/// Write a KML file for the flight with ascent/descent paths, waypoints, and points of interest
pub fn write_kml(flightname: &str, ascent: &DataFrame, descent: &DataFrame, path: &str) -> Result<()> {
    let file = File::create(path)?;
    let buf = BufWriter::new(file);
    let mut writer = Writer::new_with_indent(buf, b' ', 2);

    // XML declaration
    writer.write_event(Event::Decl(BytesDecl::new("1.0", Some("UTF-8"), None)))?;

    // <kml>
    let mut kml = BytesStart::new("kml");
    kml.push_attribute(("xmlns", "http://www.opengis.net/kml/2.2"));
    writer.write_event(Event::Start(kml))?;

    // Extract key data points
    let launch = row_data(ascent, 0)?;
    let burst = row_data(ascent, ascent.height() - 1)?;
    let landing = row_data(descent, descent.height() - 1)?;

    // Flight time computation
    let asc_pt = ascent.column("packettime")?.datetime()?;
    let desc_pt = descent.column("packettime")?.datetime()?;
    let start_ms = asc_pt.get(0).unwrap_or(0);
    let end_ms = desc_pt.get(descent.height() - 1).unwrap_or(0);
    let flight_secs = (end_ms - start_ms) as f64 / 1000.0;
    let hrs = (flight_secs / 3600.0) as i64;
    let mins = ((flight_secs - hrs as f64 * 3600.0) / 60.0) as i64;
    let secs = (flight_secs - hrs as f64 * 3600.0 - mins as f64 * 60.0) as i64;
    let flighttime = format!("{hrs}hrs {mins}mins {secs}secs");

    // <Document> (outer wrapper)
    writer.write_event(Event::Start(BytesStart::new("Document")))?;

    // Inner Document named after the flight
    writer.write_event(Event::Start(BytesStart::new("Document")))?;
    write_simple_element(&mut writer, "name", flightname)?;

    // Document description
    let desc = format!(
        r#"<![CDATA[
    <h1>{flightname}</h1>
    <table style="width: 100%;" cellpadding=0 cellspacing=0 border=0>
    <tr><td style="border: solid 1px black;border-bottom: 0; padding: 10px;"><strong>Launch Date/Time (UTC)</strong></td><td style=" padding: 10px;border: solid 1px black;border-bottom: 0; border-left: 0;">{}</td></tr>
    <tr><td style="border: solid 1px black;border-bottom: 0; padding: 10px;"><strong>Flight Duration</strong></td><td style=" padding: 10px;border: solid 1px black;border-bottom: 0; border-left: 0; ">{flighttime}</td></tr>
    <tr><td style="border: solid 1px black;border-bottom: 0; padding: 10px;"><strong>Down Range Distance</strong></td><td style=" padding: 10px;border: solid 1px black;border-bottom: 0; border-left: 0; ">{:.1}mi ({:.1}km)</td></tr>
    <tr><td style="border: solid 1px black; padding: 10px;"><strong>Approx. Burst Altitude</strong></td><td style=" padding: 10px;border: solid 1px black; border-left: 0; ">{:.0}ft ({:.1}m)</td></tr>
    </table>
    ]]>"#,
        launch.packettime_str, landing.dist_mi, landing.dist_km, burst.altitude_ft, burst.altitude_m
    );
    write_simple_element(&mut writer, "description", &desc)?;

    // Paths folder
    writer.write_event(Event::Start(BytesStart::new("Folder")))?;
    write_simple_element(&mut writer, "name", "Paths")?;

    // Ascent linestring
    write_linestring(&mut writer, &format!("{flightname} Ascent"), ascent, "ff0000ff", None)?;

    // Descent linestring (prepend burst point)
    write_linestring(&mut writer, &format!("{flightname} Descent"), descent, "ffff0000", Some(&burst))?;

    writer.write_event(Event::End(BytesEnd::new("Folder")))?; // Paths

    // Waypoints folder
    writer.write_event(Event::Start(BytesStart::new("Folder")))?;
    write_simple_element(&mut writer, "name", "Waypoints")?;

    write_altitude_waypoints(&mut writer, ascent, "ff0000ff")?; // red
    write_altitude_waypoints(&mut writer, descent, "ffff0000")?; // blue

    writer.write_event(Event::End(BytesEnd::new("Folder")))?; // Waypoints

    // Points of Interest folder
    writer.write_event(Event::Start(BytesStart::new("Folder")))?;
    write_simple_element(&mut writer, "name", "Points of Interest")?;

    // Launch point
    write_poi_point(&mut writer, "Launch", &launch, "ff00aaff", &format!(
        r#"<h1>Launch</h1>
    <table style="width: 100%;" cellpadding=0 cellspacing=0 border=0>
    <tr><td style="padding: 10px; border: solid 1px black;border-bottom: 0;"><strong>Time (UTC)</strong></td><td style="border: solid 1px black;border-bottom: 0; border-left: 0; padding: 10px;">{}</td></tr>
    <tr><td style="padding: 10px; border: solid 1px black;border-bottom: 0;"><strong>Altitude</strong></td><td style="border: solid 1px black;border-left: 0; border-bottom: 0; padding: 10px;">{:.0}ft ({:.1}m)</td></tr>
    <tr><td style="padding: 10px; border: solid 1px black;"><strong>Coordinates</strong></td><td style="border: solid 1px black; border-left: 0; padding: 10px;">{:.8}, {:.8}</td></tr>
    </table>"#,
        launch.time_str, launch.altitude_ft, launch.altitude_m, launch.lat, launch.lon
    ))?;

    // Landing point
    write_poi_point(&mut writer, "Landing", &landing, "ff00aaff", &format!(
        r#"<h1>Landing</h1>
    <table style="width: 100%;" cellpadding=0 cellspacing=0 border=0>
    <tr><td style="padding: 10px; border: solid 1px black;border-bottom: 0;"><strong>Time (UTC)</strong></td><td style="border: solid 1px black;border-left: 0; border-bottom: 0; padding: 10px;">{}</td></tr>
    <tr><td style="padding: 10px; border: solid 1px black;border-bottom: 0;"><strong>Altitude</strong></td><td style="border: solid 1px black;border-bottom: 0; border-left: 0; padding: 10px;">{:.0}ft ({:.1}m)</td></tr>
    <tr><td style="padding: 10px; border: solid 1px black;"><strong>Coordinates</strong></td><td style="border: solid 1px black; border-left: 0; padding: 10px;">{:.8}, {:.8}</td></tr>
    </table>"#,
        landing.time_str, landing.altitude_ft, landing.altitude_m, landing.lat, landing.lon
    ))?;

    // Burst point
    write_poi_point(&mut writer, &format!("Burst {:.0}ft", burst.altitude_ft), &burst, "ff00ffff", &format!(
        r#"<h1>Burst: {:.0}ft</h1>
    <table style="width: 100%;" cellpadding=0 cellspacing=0 border=0>
    <tr><td style="padding: 10px; border: solid 1px black;border-bottom: 0;"><strong>Time (UTC)</strong></td><td style="border: solid 1px black;border-left: 0; border-bottom: 0; padding: 10px;">{}</td></tr>
    <tr><td style="padding: 10px; border: solid 1px black;border-bottom: 0;"><strong>Altitude</strong></td><td style="border: solid 1px black;border-left: 0; border-bottom: 0; padding: 10px;">{:.0}ft ({:.1}m)</td></tr>
    <tr><td style="padding: 10px; border: solid 1px black;"><strong>Coordinates</strong></td><td style="border: solid 1px black; border-left: 0; padding: 10px;">{:.8}, {:.8}</td></tr>
    </table>"#,
        burst.altitude_ft, burst.time_str, burst.altitude_ft, burst.altitude_m, burst.lat, burst.lon
    ))?;

    // Reynolds transition points
    let airflow = ascent.column("reynolds_transition")?.str()?;
    let alt_ft = ascent.column("altitude_ft")?.f64()?;
    let alt_m = ascent.column("altitude_m")?.f64()?;
    let lat = ascent.column("latitude")?.f64()?;
    let lon = ascent.column("longitude")?.f64()?;
    let pt = ascent.column("packettime")?.datetime()?;

    for i in 0..ascent.height() {
        let re_trans = airflow.get(i).unwrap_or("");
        if re_trans.is_empty() {
            continue;
        }

        let re_name = if re_trans == "high_to_low" {
            "Turbulent-to-Laminar"
        } else {
            "Laminar-to-Turbulent"
        };

        let a_ft = alt_ft.get(i).unwrap_or(0.0);
        let a_m = alt_m.get(i).unwrap_or(0.0);
        let la = lat.get(i).unwrap_or(0.0);
        let lo = lon.get(i).unwrap_or(0.0);
        let t_str = ms_to_time_str(pt.get(i).unwrap_or(0));

        let rd = RowData {
            lat: la,
            lon: lo,
            altitude_ft: a_ft,
            altitude_m: a_m,
            time_str: t_str.clone(),
            packettime_str: t_str.clone(),
            dist_mi: 0.0,
            dist_km: 0.0,
        };

        let name = format!("{}k, {}", (a_ft / 1000.0).round() as i64, re_name);
        write_poi_point(&mut writer, &name, &rd, "ff006400", &format!(
            r#"<h1>Airflow Transitioning from {re_name}</h1>
        <table style="width: 100%;" cellpadding=0 cellspacing=0 border=0>
        <tr><td style="padding: 10px; border: solid 1px black;border-bottom: 0;"><strong>Time (UTC)</strong></td><td style="border: solid 1px black;border-left: 0; border-bottom: 0; padding: 10px;">{t_str}</td></tr>
        <tr><td style="padding: 10px; border: solid 1px black;border-bottom: 0;"><strong>Altitude</strong></td><td style="border: solid 1px black;border-left: 0; border-bottom: 0; padding: 10px;">{a_ft:.0}ft ({a_m:.1}m)</td></tr>
        <tr><td style="padding: 10px; border: solid 1px black;"><strong>Coordinates</strong></td><td style="border: solid 1px black; border-left: 0; padding: 10px;">{la:.8}, {lo:.8}</td></tr>
        </table>"#
        ))?;
    }

    writer.write_event(Event::End(BytesEnd::new("Folder")))?; // Points of Interest
    writer.write_event(Event::End(BytesEnd::new("Document")))?; // Root flight Document
    writer.write_event(Event::End(BytesEnd::new("Document")))?;
    writer.write_event(Event::End(BytesEnd::new("kml")))?;

    Ok(())
}

struct RowData {
    lat: f64,
    lon: f64,
    altitude_ft: f64,
    altitude_m: f64,
    time_str: String,
    packettime_str: String,
    dist_mi: f64,
    dist_km: f64,
}

fn row_data(df: &DataFrame, idx: usize) -> Result<RowData> {
    let lat = df.column("latitude")?.f64()?.get(idx).unwrap_or(0.0);
    let lon = df.column("longitude")?.f64()?.get(idx).unwrap_or(0.0);
    let alt_ft = df.column("altitude_ft")?.f64()?.get(idx).unwrap_or(0.0);
    let alt_m = df.column("altitude_m")?.f64()?.get(idx).unwrap_or(0.0);
    let pt_ms = df.column("packettime")?.datetime()?.get(idx).unwrap_or(0);
    let time_str = ms_to_time_str(pt_ms);
    let packettime_str = ms_to_datetime_str(pt_ms);
    let dist_mi = df.column("distance_from_launch_mi").ok()
        .and_then(|c| c.f64().ok())
        .and_then(|ca| ca.get(idx))
        .unwrap_or(0.0);
    let dist_km = df.column("distance_from_launch_km").ok()
        .and_then(|c| c.f64().ok())
        .and_then(|ca| ca.get(idx))
        .unwrap_or(0.0);

    Ok(RowData { lat, lon, altitude_ft: alt_ft, altitude_m: alt_m, time_str, packettime_str, dist_mi, dist_km })
}

fn ms_to_time_str(ms: i64) -> String {
    let secs = ms / 1000;
    let h = (secs / 3600) % 24;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

fn ms_to_datetime_str(ms: i64) -> String {
    let secs = ms / 1000;
    let nsecs = ((ms % 1000) * 1_000_000) as u32;
    chrono::DateTime::from_timestamp(secs, nsecs).map(|dt| dt.naive_utc())
        .map(|dt| dt.format("%Y-%m-%d %H:%M:%S").to_string())
        .unwrap_or_default()
}

fn write_simple_element<W: std::io::Write>(writer: &mut Writer<W>, tag: &str, text: &str) -> Result<()> {
    writer.write_event(Event::Start(BytesStart::new(tag)))?;
    if text.contains("<![CDATA[") {
        // Write as-is for CDATA
        writer.write_event(Event::CData(BytesCData::new(
            text.trim_start_matches("<![CDATA[").trim_end_matches("]]>")
        )))?;
    } else {
        writer.write_event(Event::Text(BytesText::new(text)))?;
    }
    writer.write_event(Event::End(BytesEnd::new(tag)))?;
    Ok(())
}

fn write_linestring<W: std::io::Write>(
    writer: &mut Writer<W>,
    name: &str,
    df: &DataFrame,
    color: &str,
    prepend: Option<&RowData>,
) -> Result<()> {
    writer.write_event(Event::Start(BytesStart::new("Placemark")))?;
    write_simple_element(writer, "name", name)?;

    // Style
    writer.write_event(Event::Start(BytesStart::new("Style")))?;
    writer.write_event(Event::Start(BytesStart::new("LineStyle")))?;
    write_simple_element(writer, "color", color)?;
    write_simple_element(writer, "width", "3")?;
    writer.write_event(Event::End(BytesEnd::new("LineStyle")))?;
    writer.write_event(Event::End(BytesEnd::new("Style")))?;

    writer.write_event(Event::Start(BytesStart::new("LineString")))?;
    write_simple_element(writer, "extrude", "0")?;
    write_simple_element(writer, "altitudeMode", "absolute")?;

    // Build coordinates string
    let lon_col = df.column("longitude")?.f64()?;
    let lat_col = df.column("latitude")?.f64()?;
    let alt_m_col = df.column("altitude_m")?.f64()?;

    let mut coords = String::new();
    if let Some(pre) = prepend {
        coords.push_str(&format!("{},{},{} ", pre.lon, pre.lat, pre.altitude_m));
    }
    for i in 0..df.height() {
        if let (Some(lo), Some(la), Some(am)) = (lon_col.get(i), lat_col.get(i), alt_m_col.get(i)) {
            coords.push_str(&format!("{lo},{la},{am} "));
        }
    }

    write_simple_element(writer, "coordinates", coords.trim())?;
    writer.write_event(Event::End(BytesEnd::new("LineString")))?;
    writer.write_event(Event::End(BytesEnd::new("Placemark")))?;

    Ok(())
}

fn write_poi_point<W: std::io::Write>(
    writer: &mut Writer<W>,
    name: &str,
    data: &RowData,
    color: &str,
    desc_html: &str,
) -> Result<()> {
    writer.write_event(Event::Start(BytesStart::new("Placemark")))?;
    write_simple_element(writer, "name", name)?;

    // Description with CDATA
    writer.write_event(Event::Start(BytesStart::new("description")))?;
    writer.write_event(Event::CData(BytesCData::new(desc_html)))?;
    writer.write_event(Event::End(BytesEnd::new("description")))?;

    // Style
    writer.write_event(Event::Start(BytesStart::new("Style")))?;
    writer.write_event(Event::Start(BytesStart::new("IconStyle")))?;
    write_simple_element(writer, "color", color)?;
    writer.write_event(Event::Start(BytesStart::new("Icon")))?;
    write_simple_element(writer, "href", "https://maps.google.com/mapfiles/kml/shapes/placemark_circle.png")?;
    writer.write_event(Event::End(BytesEnd::new("Icon")))?;
    writer.write_event(Event::End(BytesEnd::new("IconStyle")))?;
    writer.write_event(Event::End(BytesEnd::new("Style")))?;

    // Point
    writer.write_event(Event::Start(BytesStart::new("Point")))?;
    write_simple_element(writer, "altitudeMode", "absolute")?;
    write_simple_element(writer, "coordinates", &format!("{},{},{}", data.lon, data.lat, data.altitude_m))?;
    writer.write_event(Event::End(BytesEnd::new("Point")))?;

    writer.write_event(Event::End(BytesEnd::new("Placemark")))?;
    Ok(())
}

fn write_altitude_waypoints<W: std::io::Write>(
    writer: &mut Writer<W>,
    df: &DataFrame,
    color: &str,
) -> Result<()> {
    let alt_ft = df.column("altitude_ft")?.f64()?;
    let alt_m = df.column("altitude_m")?.f64()?;
    let lat = df.column("latitude")?.f64()?;
    let lon = df.column("longitude")?.f64()?;
    let pt = df.column("packettime")?.datetime()?;

    // Find min and max altitude
    let mut min_alt = f64::INFINITY;
    let mut max_alt = f64::NEG_INFINITY;
    for i in 0..df.height() {
        if let Some(a) = alt_ft.get(i) {
            min_alt = min_alt.min(a);
            max_alt = max_alt.max(a);
        }
    }

    // Target altitudes at 10,000 ft intervals
    let start = ((min_alt / 10000.0).ceil() * 10000.0) as i64;
    let end = max_alt as i64;

    let mut target = start;
    while target < end {
        let target_f = target as f64;

        // Find nearest point to this target altitude
        let mut best_idx = 0usize;
        let mut best_diff = f64::INFINITY;
        for i in 0..df.height() {
            if let Some(a) = alt_ft.get(i) {
                let diff = (a - target_f).abs();
                if diff < best_diff {
                    best_diff = diff;
                    best_idx = i;
                }
            }
        }

        let la = lat.get(best_idx).unwrap_or(0.0);
        let lo = lon.get(best_idx).unwrap_or(0.0);
        let a_ft = alt_ft.get(best_idx).unwrap_or(0.0);
        let a_m = alt_m.get(best_idx).unwrap_or(0.0);
        let t_str = ms_to_time_str(pt.get(best_idx).unwrap_or(0));

        let rd = RowData {
            lat: la,
            lon: lo,
            altitude_ft: a_ft,
            altitude_m: a_m,
            time_str: t_str.clone(),
            packettime_str: String::new(),
            dist_mi: 0.0,
            dist_km: 0.0,
        };

        let name = format!("{}k", target / 1000);
        write_poi_point(writer, &name, &rd, color, &format!(
            r#"<h1>Waypoint</h1>
        <table style="width: 100%;" cellpadding=0 cellspacing=0 border=0>
        <tr><td style="padding: 10px; border: solid 1px black;border-bottom: 0;"><strong>Time (UTC)</strong></td><td style="border: solid 1px black;border-left: 0; border-bottom: 0; padding: 10px;">{t_str}</td></tr>
        <tr><td style="padding: 10px; border: solid 1px black;border-bottom: 0;"><strong>Altitude</strong></td><td style="border: solid 1px black;border-left: 0; border-bottom: 0; padding: 10px;">{a_ft:.0}ft ({a_m:.1}m)</td></tr>
        <tr><td style="padding: 10px; border: solid 1px black;"><strong>Coordinates</strong></td><td style="border: solid 1px black; border-left: 0; padding: 10px;">{la:.8}, {lo:.8}</td></tr>
        </table>"#
        ))?;

        target += 10000;
    }

    Ok(())
}
