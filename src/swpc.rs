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

pub async fn solar_wind_payload() -> Vec<Value> {
    let solarwind_url = "https://services.swpc.noaa.gov/products/geospace/propagated-solar-wind-1-hour.json";

    let response = reqwest::get(solarwind_url)
        .await
        .unwrap()
        .json::<Value>()
        .await
        .unwrap()
        .as_array()
        .unwrap()
        .par_iter()
        .skip(1)
        .map(|x| x.clone())
        .collect::<Vec<Value>>();

    response
}

pub fn payload_to_solarwind(response: Vec<Value>) -> Vec<SolarWind> {
      
    response
        .par_iter()
        .map(|x| SolarWind {
            timestamp: NaiveDateTime::parse_from_str(&x[0].to_string().replace("\"", ""), "%Y-%m-%d %H:%M:%S%.3f").unwrap().and_utc().timestamp(),
            time_tag: x[0].as_str().unwrap().to_string(),
            speed: x[1].to_string().replace("\"", "").parse::<f64>().unwrap_or(0.0),
            density: x[2].to_string().replace("\"", "").parse::<f64>().unwrap_or(0.0),
            temperature: x[3].to_string().replace("\"", "").parse::<f64>().unwrap_or(0.0),
            bt: x[7].to_string().replace("\"", "").parse::<f64>().unwrap_or(0.0),
            bz: x[6].to_string().replace("\"", "").parse::<f64>().unwrap_or(0.0)
        }).collect::<Vec<SolarWind>>()
}

pub async fn filtered_solar_wind_data(timestamp: i64, solar_wind: Vec<SolarWind>) -> Vec<SolarWind> {

    let filtered_solar_wind = solar_wind
        .into_par_iter()
        .filter(|x| x.timestamp > timestamp)
        .collect::<Vec<SolarWind>>();

    filtered_solar_wind

}