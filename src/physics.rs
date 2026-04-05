use nalgebra::{DMatrix, DVector};

/// Haversine distance between two lat/lon points, in miles
pub fn haversine_distance(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    let lat1 = lat1.to_radians();
    let lon1 = lon1.to_radians();
    let lat2 = lat2.to_radians();
    let lon2 = lon2.to_radians();

    let dlat = lat2 - lat1;
    let dlon = lon2 - lon1;

    let a = (dlat / 2.0).sin().powi(2) + lat1.cos() * lat2.cos() * (dlon / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().asin();

    let r = 3956.0; // miles
    c * r
}

/// Air density in kg/m³ from pressure (Pa) and temperature (K)
pub fn air_density_kgm3(pressure_pa: f64, temperature_k: f64) -> f64 {
    pressure_pa / (287.05 * temperature_k)
}

/// Air density in slugs/ft³
pub fn air_density_slugs(pressure_pa: f64, temperature_k: f64) -> f64 {
    air_density_kgm3(pressure_pa, temperature_k) / 515.3788199999872
}

/// Perpendicular distance from a point (x, y) to the line y = mx + b
/// Signed: positive means below the line, negative means above
pub fn distance_to_line(x: f64, y: f64, m: f64, b: f64) -> f64 {
    (m * x - y + b) / (m * m + 1.0).sqrt()
}

/// Determine polynomial degree based on VMR (Variance-to-Mean Ratio)
pub fn vmr_degree(vmr: f64, max_degree: i32) -> usize {
    let deg = if vmr > 1.75 {
        ((max_degree as f64 + vmr) / vmr) as i32
    } else if vmr >= 1.0 {
        (max_degree as f64 / vmr) as i32
    } else {
        max_degree
    };
    deg.max(1) as usize
}

/// Polynomial fit replicating numpy's Polynomial.fit() behavior.
///
/// Maps the x domain to [-1, 1] for numerical stability (matching numpy),
/// then solves the least-squares system using SVD.
///
/// Returns a closure that evaluates the polynomial at given x values.
pub fn polynomial_fit(x: &[f64], y: &[f64], degree: usize) -> Box<dyn Fn(f64) -> f64 + Send + Sync> {
    let n = x.len();
    assert!(n > degree, "Need more points than polynomial degree");

    // Map x to [-1, 1] like numpy does
    let x_min = x.iter().cloned().fold(f64::INFINITY, f64::min);
    let x_max = x.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
    let x_range = x_max - x_min;
    let x_mid = (x_min + x_max) / 2.0;
    let half_range = x_range / 2.0;

    let x_mapped: Vec<f64> = if half_range.abs() < 1e-15 {
        vec![0.0; n]
    } else {
        x.iter().map(|&xi| (xi - x_mid) / half_range).collect()
    };

    // Build Vandermonde matrix in mapped domain
    let rows = n;
    let cols = degree + 1;
    let mut vandermonde = DMatrix::zeros(rows, cols);
    for i in 0..rows {
        let mut val = 1.0;
        for j in 0..cols {
            vandermonde[(i, j)] = val;
            val *= x_mapped[i];
        }
    }

    let y_vec = DVector::from_column_slice(y);

    // Solve via SVD least squares
    let svd = vandermonde.svd(true, true);
    let coeffs = svd.solve(&y_vec, 1e-14).unwrap_or_else(|_| {
        // Fallback: return zeros
        DVector::zeros(cols)
    });

    let coeffs_vec: Vec<f64> = coeffs.iter().cloned().collect();

    // Return evaluation closure that maps input x back to [-1, 1] before evaluating
    Box::new(move |xi: f64| {
        let mapped = if half_range.abs() < 1e-15 {
            0.0
        } else {
            (xi - x_mid) / half_range
        };
        let mut result = 0.0;
        let mut power = 1.0;
        for &c in &coeffs_vec {
            result += c * power;
            power *= mapped;
        }
        result
    })
}
