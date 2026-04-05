use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Raw flight entry from flightlist.json
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct FlightInput {
    pub flight: String,
    pub beacons: Vec<String>,
    pub day: String,
    pub balloonsize: String,
    pub parachute: ParachuteInput,
    pub weights: HashMap<String, String>,
    pub liftfactor: String,
    pub h2fill: String,
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct ParachuteInput {
    pub description: String,
    pub size: String,
}

/// Enriched parachute with unit conversions
#[derive(Debug, Clone, Serialize)]
pub struct Parachute {
    pub description: String,
    pub size_ft: f64,
    pub size_m: f64,
    pub weight_lb: Option<f64>,
    pub weight_kg: Option<f64>,
}

/// Enriched weights with lb/kg pairs
pub type Weights = HashMap<String, f64>;

/// Location point
#[derive(Debug, Clone, Serialize)]
pub struct Location {
    pub latitude: f64,
    pub longitude: f64,
    pub altitude_ft: f64,
    pub altitude_m: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distance_from_launch_mi: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distance_from_launch_km: Option<f64>,
}

/// Detected burst info
#[derive(Debug, Clone, Serialize)]
pub struct DetectedBurst {
    pub detected: bool,
    pub burst_ft: f64,
    pub burst_m: f64,
}

/// Reynolds transition record
#[derive(Debug, Clone, Serialize)]
pub struct ReynoldsTransition {
    pub transition: String,
    pub altitude_ft: f64,
    pub altitude_m: f64,
}

/// Full flight metadata for output
#[derive(Debug, Clone, Serialize)]
pub struct FlightMetadata {
    pub flight: String,
    pub beacons: Vec<String>,
    pub day: String,
    pub balloonsize: String,
    pub parachute: Parachute,
    pub weights: Weights,
    pub liftfactor: String,
    pub h2fill: String,
    pub maxaltitude_ft: f64,
    pub maxaltitude_m: f64,
    pub detected_burst: DetectedBurst,
    pub numpoints: usize,
    pub flighttime: String,
    pub flighttime_secs: f64,
    pub range_distance_traveled_mi: f64,
    pub range_distance_traveled_km: f64,
    pub launch_location: Location,
    pub landing_location: Location,
    pub reynolds_transitions: Vec<ReynoldsTransition>,
}

/// Convert raw FlightInput into enriched weights and parachute
pub fn convert_units(input: &FlightInput) -> (Weights, Parachute) {
    let mut weights = Weights::new();
    let mut parachute_weight_lb = None;
    let mut parachute_weight_kg = None;

    for (key, val) in &input.weights {
        let value_lb: f64 = val.parse().unwrap_or(0.0);
        let value_kg = (value_lb * 0.4535924 * 100.0).round() / 100.0;
        weights.insert(format!("{key}_lb"), value_lb);
        weights.insert(format!("{key}_kg"), value_kg);

        if key == "parachute" {
            parachute_weight_lb = Some(value_lb);
            parachute_weight_kg = Some(value_kg);
        }
    }

    let size_ft: f64 = input.parachute.size.parse().unwrap_or(0.0);
    let parachute = Parachute {
        description: input.parachute.description.clone(),
        size_ft,
        size_m: (size_ft * 0.3048 * 100.0).round() / 100.0,
        weight_lb: parachute_weight_lb,
        weight_kg: parachute_weight_kg,
    };

    (weights, parachute)
}
