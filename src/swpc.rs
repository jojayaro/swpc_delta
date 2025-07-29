use chrono::NaiveDateTime;
use serde::{Serialize, Deserialize};
use rayon::prelude::*;
use serde_json::Value;
use crate::error::Result;

#[derive(Debug, Serialize, Deserialize)]
pub struct Magnetometer {
    pub timestamp: i64,
    pub time_tag: String,
    pub satellite: u8,
    pub he: f64,
    pub hp: f64,
    pub hn: f64,
    pub total: f64,
    pub arcjet_flag: bool,
}

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

pub async fn solar_wind_payload() -> Result<Vec<Value>> {
    let solarwind_url = "https://services.swpc.noaa.gov/products/geospace/propagated-solar-wind.json";

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

pub async fn magnetometer_payload() -> Result<Vec<Value>> {
    let url = "https://services.swpc.noaa.gov/json/goes/primary/magnetometers-6-hour.json";
    let response = reqwest::get(url).await?.json::<Value>().await?;
    Ok(response.as_array().unwrap_or(&vec![]).par_iter().map(|x| x.clone()).collect())
}

pub fn payload_to_solarwind(response: Vec<Value>) -> Result<Vec<SolarWind>> {
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

pub fn payload_to_magnetometer(response: Vec<Value>) -> Result<Vec<Magnetometer>> {
    let mut result = Vec::with_capacity(response.len());
    for entry in response.iter() {
        let time_tag = entry["time_tag"].as_str().unwrap_or("").to_string();
        let timestamp = NaiveDateTime::parse_from_str(&time_tag, "%Y-%m-%dT%H:%M:%SZ")?
            .and_utc()
            .timestamp();
        
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

pub async fn filtered_solar_wind_data(timestamp: i64, solar_wind: Vec<SolarWind>) -> Vec<SolarWind> {
    solar_wind
        .into_par_iter()
        .filter(|x| x.timestamp > timestamp)
        .collect()
}

pub async fn filtered_magnetometer_data(timestamp: i64, magnetometer: Vec<Magnetometer>) -> Vec<Magnetometer> {
    magnetometer
        .into_par_iter()
        .filter(|x| x.timestamp > timestamp)
        .collect()
}
