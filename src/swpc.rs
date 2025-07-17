use chrono::NaiveDateTime;
use serde::{Serialize, Deserialize};
use serde_json::Value;
use rayon::prelude::*;

#[derive(Debug, Serialize, Deserialize)]
pub struct SolarWind {
    pub timestamp: i64,
    pub time_tag: String,
    pub speed: f64,
    pub density: f64,
    pub temperature: f64,
    pub bt: f64,
    pub bz: f64
}

pub async fn solar_wind_payload() -> Result<Vec<Value>, reqwest::Error> {
    let solarwind_url = "https://services.swpc.noaa.gov/products/geospace/propagated-solar-wind-1-hour.json";

    let response = reqwest::get(solarwind_url)
        .await?
        .json::<Value>()
        .await?;

    let array = match response.as_array() {
        Some(arr) => arr,
        None => return Ok(vec![]),
    };

    let result = array
        .par_iter()
        .skip(1)
        .map(|x| x.clone())
        .collect::<Vec<Value>>();

    Ok(result)
}

pub fn payload_to_solarwind(response: Vec<Value>) -> Result<Vec<SolarWind>, Box<dyn std::error::Error>> {
    let mut result = Vec::with_capacity(response.len());
    for x in response.iter() {
        let timestamp = NaiveDateTime::parse_from_str(&x[0].to_string().replace("\"", ""), "%Y-%m-%d %H:%M:%S%.3f")?
            .and_utc()
            .timestamp();
        let time_tag = x[0].as_str().unwrap_or("").to_string();
        let speed = x[1].to_string().replace("\"", "").parse::<f64>().unwrap_or(0.0);
        let density = x[2].to_string().replace("\"", "").parse::<f64>().unwrap_or(0.0);
        let temperature = x[3].to_string().replace("\"", "").parse::<f64>().unwrap_or(0.0);
        let bt = x[7].to_string().replace("\"", "").parse::<f64>().unwrap_or(0.0);
        let bz = x[6].to_string().replace("\"", "").parse::<f64>().unwrap_or(0.0);

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

pub async fn filtered_solar_wind_data(timestamp: i64, solar_wind: Vec<SolarWind>) -> Vec<SolarWind> {

    let filtered_solar_wind = solar_wind
        .into_par_iter()
        .filter(|x| x.timestamp > timestamp)
        .collect::<Vec<SolarWind>>();

    filtered_solar_wind

}
