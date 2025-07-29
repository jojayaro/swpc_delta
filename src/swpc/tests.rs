use super::*;
use proptest::prelude::*;
use serde_json::json;

#[test]
fn test_parse_float_value() {
    assert_eq!(parse_float_value(&json!(42.5)), 42.5);
    assert_eq!(parse_float_value(&json!("123.45")), 123.45);
    assert_eq!(parse_float_value(&json!("invalid")), 0.0);
    assert_eq!(parse_float_value(&json!(null)), 0.0);
    assert_eq!(parse_float_value(&json!(true)), 0.0);
}

#[test]
fn test_payload_to_solarwind_valid_data() {
    let payload = vec![json!([
        "2024-01-01 12:00:00.000",
        "400.5",
        "5.2",
        "100000",
        "0.0",
        "0.0",
        "-2.5",
        "8.7"
    ])];

    let result = payload_to_solarwind(payload).unwrap();
    assert_eq!(result.len(), 1);

    let solar_wind = &result[0];
    assert_eq!(solar_wind.speed, 400.5);
    assert_eq!(solar_wind.density, 5.2);
    assert_eq!(solar_wind.temperature, 100000.0);
    assert_eq!(solar_wind.bz, -2.5);
    assert_eq!(solar_wind.bt, 8.7);
}

#[test]
fn test_payload_to_solarwind_malformed_data() {
    // Test with insufficient array length
    let payload = vec![json!(["2024-01-01 12:00:00.000", "400.5"])];
    let result = payload_to_solarwind(payload).unwrap();
    assert_eq!(result.len(), 0); // Should skip malformed entries

    // Test with invalid timestamp
    let payload = vec![json!([
        "invalid-date",
        "400.5",
        "5.2",
        "100000",
        "0.0",
        "0.0",
        "-2.5",
        "8.7"
    ])];
    let result = payload_to_solarwind(payload).unwrap();
    assert_eq!(result.len(), 0); // Should skip entries with invalid timestamps
}

#[test]
fn test_payload_to_magnetometer_valid_data() {
    let payload = vec![json!({
        "time_tag": "2024-01-01T12:00:00Z",
        "satellite": 16,
        "He": 120.5,
        "Hp": 45.3,
        "Hn": -78.9,
        "total": 150.2,
        "arcjet_flag": true
    })];

    let result = payload_to_magnetometer(payload).unwrap();
    assert_eq!(result.len(), 1);

    let mag = &result[0];
    assert_eq!(mag.satellite, 16);
    assert_eq!(mag.he, 120.5);
    assert_eq!(mag.hp, 45.3);
    assert_eq!(mag.hn, -78.9);
    assert_eq!(mag.total, 150.2);
    assert_eq!(mag.arcjet_flag, true);
}

#[test]
fn test_payload_to_magnetometer_malformed_data() {
    // Test with empty time_tag
    let payload = vec![json!({
        "time_tag": "",
        "satellite": 16,
        "He": 120.5,
        "Hp": 45.3,
        "Hn": -78.9,
        "total": 150.2,
        "arcjet_flag": true
    })];
    let result = payload_to_magnetometer(payload).unwrap();
    assert_eq!(result.len(), 0); // Should skip entries with empty time_tag

    // Test with invalid timestamp format
    let payload = vec![json!({
        "time_tag": "invalid-timestamp",
        "satellite": 16,
        "He": 120.5,
        "Hp": 45.3,
        "Hn": -78.9,
        "total": 150.2,
        "arcjet_flag": true
    })];
    let result = payload_to_magnetometer(payload).unwrap();
    assert_eq!(result.len(), 0); // Should skip entries with invalid timestamps
}

#[tokio::test]
async fn test_filtered_solar_wind_data() {
    let solar_wind = vec![
        SolarWind {
            timestamp: 1000,
            time_tag: "2024-01-01 12:00:00.000".to_string(),
            speed: 400.0,
            density: 5.0,
            temperature: 100000.0,
            bt: 8.0,
            bz: -2.0,
        },
        SolarWind {
            timestamp: 2000,
            time_tag: "2024-01-01 12:01:00.000".to_string(),
            speed: 450.0,
            density: 5.5,
            temperature: 110000.0,
            bt: 9.0,
            bz: -3.0,
        },
    ];

    let filtered = filtered_solar_wind_data(1500, solar_wind).await;
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].timestamp, 2000);
}

#[tokio::test]
async fn test_filtered_magnetometer_data() {
    let magnetometer = vec![
        Magnetometer {
            timestamp: 1000,
            time_tag: "2024-01-01T12:00:00Z".to_string(),
            satellite: 16,
            he: 120.0,
            hp: 45.0,
            hn: -78.0,
            total: 150.0,
            arcjet_flag: false,
        },
        Magnetometer {
            timestamp: 2000,
            time_tag: "2024-01-01T12:01:00Z".to_string(),
            satellite: 16,
            he: 125.0,
            hp: 50.0,
            hn: -80.0,
            total: 155.0,
            arcjet_flag: true,
        },
    ];

    let filtered = filtered_magnetometer_data(1500, magnetometer).await;
    assert_eq!(filtered.len(), 1);
    assert_eq!(filtered[0].timestamp, 2000);
}

// Property-based tests
proptest! {
    #[test]
    fn test_parse_float_value_properties(
        value in prop::option::of(prop::num::f64::NORMAL)
    ) {
        let json_value = match value {
            Some(v) => json!(v),
            None => json!(null),
        };

        let result = parse_float_value(&json_value);

        // Result should always be a valid f64
        prop_assert!(result.is_finite() || result == 0.0);
    }

    #[test]
    fn test_filtered_solar_wind_timestamp_filtering(
        threshold in 0i64..1000000i64,
        timestamps in prop::collection::vec(0i64..1000000i64, 0..100)
    ) {
        let runtime = tokio::runtime::Runtime::new().unwrap();

        let solar_wind: Vec<SolarWind> = timestamps.into_iter().map(|ts| SolarWind {
            timestamp: ts,
            time_tag: "2024-01-01 12:00:00.000".to_string(),
            speed: 400.0,
            density: 5.0,
            temperature: 100000.0,
            bt: 8.0,
            bz: -2.0,
        }).collect();

        let filtered = runtime.block_on(filtered_solar_wind_data(threshold, solar_wind.clone()));

        // All filtered items should have timestamp > threshold
        for item in &filtered {
            prop_assert!(item.timestamp > threshold);
        }

        // Count should match manual filter
        let expected_count = solar_wind.iter().filter(|sw| sw.timestamp > threshold).count();
        prop_assert_eq!(filtered.len(), expected_count);
    }
}
