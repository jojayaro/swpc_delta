
use swpc_delta::delta::{create_initialized_table_overwrite, optimize_delta, vacuum_delta, max_solar_wind_timestamp, solar_wind_to_batch};
use swpc_delta::swpc::{payload_to_solarwind, solar_wind_payload};
use deltalake::writer::{RecordBatchWriter, DeltaWriter};
use deltalake::Path;

#[tokio::test]
async fn test_table_creation() {
    let table_uri = "./test_table_creation".to_string();
    let table_path = Path::from(table_uri.as_ref());

    let _ = std::fs::remove_dir_all(&table_uri);
    let table = create_initialized_table_overwrite(&table_path).await.unwrap();
    assert!(table.version().unwrap() >= 1);

    // Clean up
    let _ = std::fs::remove_dir_all(&table_uri);
}

#[tokio::test]
async fn test_data_ingestion() {
    let table_uri = "./test_data_ingestion".to_string();
    let table_path = Path::from(table_uri.as_ref());

    let mut table = create_initialized_table_overwrite(&table_path).await.unwrap();

    let solar_wind_data = payload_to_solarwind(solar_wind_payload().await);
    let batch = solar_wind_to_batch(&table, solar_wind_data).await;

    let mut writer = RecordBatchWriter::for_table(&table).expect("Failed to make RecordBatchWriter");
    writer.write(batch).await.unwrap();
    writer.flush_and_commit(&mut table).await.unwrap();

    assert!(table.version().unwrap() >= 1);

    // Clean up
    let _ = std::fs::remove_dir_all(&table_uri);
}

#[tokio::test]
async fn test_optimize_and_vacuum() {
    let table_uri = "./test_optimize_and_vacuum".to_string();
    let table_path = Path::from(table_uri.as_ref());

    let mut table = create_initialized_table_overwrite(&table_path).await.unwrap();

    // Ingest some data to optimize and vacuum
    let solar_wind_data = payload_to_solarwind(solar_wind_payload().await);
    let batch = solar_wind_to_batch(&table, solar_wind_data).await;

    let mut writer = RecordBatchWriter::for_table(&table).expect("Failed to make RecordBatchWriter");
    writer.write(batch).await.unwrap();
    writer.flush_and_commit(&mut table).await.unwrap();

    optimize_delta(&table_path).await;
    vacuum_delta(&table_path).await;

    // Re-open table to check version after optimize/vacuum
    let table = deltalake::open_table(&table_path).await.unwrap();
    assert!(table.version().unwrap() >= 1);

    // Clean up
    let _ = std::fs::remove_dir_all(&table_uri);
}

#[tokio::test]
async fn test_max_solar_wind_timestamp() {
    let table_uri = "./test_max_solar_wind_timestamp".to_string();
    let table_path = Path::from(table_uri.as_ref());

    let mut table = create_initialized_table_overwrite(&table_path).await.unwrap();

    // Ingest some data
    let solar_wind_data = payload_to_solarwind(solar_wind_payload().await);
    let batch = solar_wind_to_batch(&table, solar_wind_data).await;

    let mut writer = RecordBatchWriter::for_table(&table).expect("Failed to make RecordBatchWriter");
    writer.write(batch).await.unwrap();
    writer.flush_and_commit(&mut table).await.unwrap();

    match max_solar_wind_timestamp(table_uri.clone()).await {
        ts if ts > 0 => assert!(ts > 0),
        _ => panic!("max_solar_wind_timestamp failed or returned invalid value"),
    }

    // Clean up
    let _ = std::fs::remove_dir_all(&table_uri);
}
