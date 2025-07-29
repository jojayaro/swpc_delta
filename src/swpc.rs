use crate::error::Result;
use chrono::NaiveDateTime;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use serde_json::Value;

#[cfg(test)]
mod tests;

/// GOES magnetometer data structure
///
/// Contains magnetic field measurements from GOES satellites.
/// Data source: https://services.swpc.noaa.gov/json/goes/primary/magnetometers-6-hour.json
///
/// # Examples
///
/// ```rust
/// use swpc_delta::swpc::Magnetometer;
///
/// let mag = Magnetometer {
///     timestamp: 1640995200,
///     time_tag: "2022-01-01T00:00:00Z".to_string(),
///     satellite: 16,
///     he: 120.5,
///     hp: 45.3,
///     hn: -78.9,
///     total: 150.2,
///     arcjet_flag: false,
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Magnetometer {
    /// Unix timestamp (seconds since epoch)
    pub timestamp: i64,
    /// ISO 8601 formatted timestamp string
    pub time_tag: String,
    /// GOES satellite number (typically 16 or 18)
    pub satellite: u8,
    /// Magnetic field component He (nT)
    pub he: f64,
    /// Magnetic field component Hp (nT)
    pub hp: f64,
    /// Magnetic field component Hn (nT)
    pub hn: f64,
    /// Total magnetic field strength (nT)
    pub total: f64,
    /// Indicates if arcjet thrusters were firing (affects measurements)
    pub arcjet_flag: bool,
}

/// Solar wind plasma data structure
///
/// Contains real-time solar wind measurements including speed, density, temperature, and magnetic field.
/// Data source: https://services.swpc.noaa.gov/products/geospace/propagated-solar-wind.json
///
/// # Examples
///
/// ```rust
/// use swpc_delta::swpc::SolarWind;
///
/// let sw = SolarWind {
///     timestamp: 1640995200,
///     time_tag: "2022-01-01 00:00:00.000".to_string(),
///     speed: 400.5,
///     density: 5.2,
///     temperature: 100000.0,
///     bt: 8.7,
///     bz: -2.5,
/// };
/// ```
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SolarWind {
    /// Unix timestamp (seconds since epoch)
    pub timestamp: i64,
    /// Formatted timestamp string
    pub time_tag: String,
    /// Solar wind speed (km/s)
    pub speed: f64,
    /// Solar wind proton density (protons/cm³)
    pub density: f64,
    /// Solar wind temperature (K)
    pub temperature: f64,
    /// Total magnetic field strength (nT)
    pub bt: f64,
    /// North-south component of magnetic field (nT) - important for geomagnetic activity
    pub bz: f64,
}

/// Fetch solar wind data from SWPC API
///
/// Retrieves the latest solar wind measurements from the Space Weather Prediction Center.
/// Uses conditional parallelism based on data size for optimal performance.
///
/// # Returns
///
/// A vector of JSON values representing solar wind measurements, or an error if the request fails.
///
/// # Errors
///
/// Returns `SwpcDeltaError::Http` if the HTTP request fails.
/// Returns `SwpcDeltaError::Json` if JSON parsing fails.
///
/// # Examples
///
/// ```rust
/// use swpc_delta::swpc::solar_wind_payload;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     let payload = solar_wind_payload().await?;
///     println!("Retrieved {} solar wind records", payload.len());
///     Ok(())
/// }
/// ```
pub async fn solar_wind_payload() -> Result<Vec<Value>> {
    let solarwind_url =
        "https://services.swpc.noaa.gov/products/geospace/propagated-solar-wind.json";

    let response = reqwest::get(solarwind_url).await?.json::<Value>().await?;

    let array = match response.as_array() {
        Some(arr) => arr,
        None => return Ok(vec![]),
    };

    // Use parallel processing only for large datasets (>1000 entries)
    let result = if array.len() > 1000 {
        array
            .par_iter()
            .skip(1)
            .map(|x| x.clone())
            .collect::<Vec<Value>>()
    } else {
        array.iter().skip(1).cloned().collect::<Vec<Value>>()
    };

    Ok(result)
}

pub async fn magnetometer_payload() -> Result<Vec<Value>> {
    let url = "https://services.swpc.noaa.gov/json/goes/primary/magnetometers-6-hour.json";
    let response = reqwest::get(url).await?.json::<Value>().await?;

    let empty_array = vec![];
    let array = response.as_array().unwrap_or(&empty_array);

    // Use parallel processing only for large datasets (>1000 entries)
    let result = if array.len() > 1000 {
        array.par_iter().map(|x| x.clone()).collect()
    } else {
        array.to_vec()
    };

    Ok(result)
}

pub fn payload_to_solarwind(response: Vec<Value>) -> Result<Vec<SolarWind>> {
    // Use iterator map with pre-allocated capacity for better memory efficiency
    let mut result = Vec::with_capacity(response.len());

    // Early validation and filter malformed entries
    for x in response.iter() {
        // Validate array has minimum required elements
        if x.as_array().is_none_or(|arr| arr.len() < 8) {
            continue; // Skip malformed entries
        }

        // Parse timestamp with better error handling
        let time_tag = x[0].as_str().unwrap_or("").to_string();
        let timestamp = match NaiveDateTime::parse_from_str(
            &time_tag.replace("\"", ""),
            "%Y-%m-%d %H:%M:%S%.3f",
        ) {
            Ok(dt) => dt.and_utc().timestamp(),
            Err(_) => continue, // Skip entries with invalid timestamps
        };

        // Extract numeric values with validation
        let speed = parse_float_value(&x[1]);
        let density = parse_float_value(&x[2]);
        let temperature = parse_float_value(&x[3]);
        let bt = parse_float_value(&x[7]);
        let bz = parse_float_value(&x[6]);

        result.push(SolarWind {
            timestamp,
            time_tag,
            speed,
            density,
            temperature,
            bt,
            bz,
        });
    }
    Ok(result)
}

/// Helper function to parse float values with better error handling
pub fn parse_float_value(value: &Value) -> f64 {
    match value {
        Value::Number(n) => n.as_f64().unwrap_or(0.0),
        Value::String(s) => s.parse::<f64>().unwrap_or(0.0),
        _ => 0.0,
    }
}

pub fn payload_to_magnetometer(response: Vec<Value>) -> Result<Vec<Magnetometer>> {
    let mut result = Vec::with_capacity(response.len());

    // Early validation and filter malformed entries
    for entry in response.iter() {
        let time_tag = entry["time_tag"].as_str().unwrap_or("").to_string();

        // Skip entries with empty time_tag
        if time_tag.is_empty() {
            continue;
        }

        // Parse timestamp with better error handling
        let timestamp = match NaiveDateTime::parse_from_str(&time_tag, "%Y-%m-%dT%H:%M:%SZ") {
            Ok(dt) => dt.and_utc().timestamp(),
            Err(_) => continue, // Skip entries with invalid timestamps
        };

        result.push(Magnetometer {
            timestamp,
            time_tag,
            satellite: entry["satellite"].as_u64().unwrap_or(0) as u8,
            he: entry["He"].as_f64().unwrap_or(0.0),
            hp: entry["Hp"].as_f64().unwrap_or(0.0),
            hn: entry["Hn"].as_f64().unwrap_or(0.0),
            total: entry["total"].as_f64().unwrap_or(0.0),
            arcjet_flag: entry["arcjet_flag"].as_bool().unwrap_or(false),
        });
    }
    Ok(result)
}

pub async fn filtered_solar_wind_data(
    timestamp: i64,
    solar_wind: Vec<SolarWind>,
) -> Vec<SolarWind> {
    // Use parallel processing only for large datasets (>1000 entries)
    if solar_wind.len() > 1000 {
        solar_wind
            .into_par_iter()
            .filter(|x| x.timestamp > timestamp)
            .collect()
    } else {
        solar_wind
            .into_iter()
            .filter(|x| x.timestamp > timestamp)
            .collect()
    }
}

pub async fn filtered_magnetometer_data(
    timestamp: i64,
    magnetometer: Vec<Magnetometer>,
) -> Vec<Magnetometer> {
    // Use parallel processing only for large datasets (>1000 entries)
    if magnetometer.len() > 1000 {
        magnetometer
            .into_par_iter()
            .filter(|x| x.timestamp > timestamp)
            .collect()
    } else {
        magnetometer
            .into_iter()
            .filter(|x| x.timestamp > timestamp)
            .collect()
    }
}
