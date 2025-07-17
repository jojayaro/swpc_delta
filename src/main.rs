use deltalake::{DeltaTableError, writer::{RecordBatchWriter, DeltaWriter}};
use swpc_delta::{
    delta::{create_initialized_table, max_solar_wind_timestamp, solar_wind_to_batch, optimize_delta, vacuum_delta}, 
    swpc::{filtered_solar_wind_data, payload_to_solarwind, solar_wind_payload}
};
use log::{info, error};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    env_logger::init();

    let table_uri = "/solar_wind/".to_string();

    info!("Attempting to open Delta Lake table at: {}", table_uri);
    let table_path = deltalake::Path::from(table_uri.as_ref());

    let maybe_table = deltalake::open_table(&table_path).await;
    let mut table = match maybe_table {
        Ok(table) => {
            info!("Successfully opened existing Delta Lake table.");
            table
        },
        Err(DeltaTableError::NotATable(_)) => {
            info!("Delta Lake table not found. Creating a new one.");
            match create_initialized_table(&table_path).await {
                Ok(table) => table,
                Err(err) => {
                    error!("Failed to create Delta Lake table: {}", err);
                    return Err(Box::new(err) as Box<dyn std::error::Error>);
                }
            }
        }
        Err(err) => {
            error!("Failed to open Delta Lake table: {}", err);
            return Err(Box::new(err));
        },
    };

    info!("Fetching max solar wind timestamp.");
    let timestamp = max_solar_wind_timestamp("./solar_wind".to_string()).await;
    info!("Max solar wind timestamp: {}", timestamp);

    info!("Fetching solar wind payload.");
    let solar_wind_payload_data = solar_wind_payload().await?;
    info!("Filtering solar wind data.");
    let solar_wind = filtered_solar_wind_data(timestamp, payload_to_solarwind(solar_wind_payload_data)?).await;

    if solar_wind.len() > 0 {
        info!("{} new solar wind records found. Ingesting data.", solar_wind.len());
        let batch = solar_wind_to_batch(&table, solar_wind).await;

        let mut writer = RecordBatchWriter::for_table(&table).map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
        writer.write(batch).await.map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;
        writer
            .flush_and_commit(&mut table)
            .await
            .map_err(|e| Box::new(e) as Box<dyn std::error::Error>)?;

        info!("Data ingestion complete.");
    } else {
        info!("No new solar wind records to ingest.");
    }

    info!("Optimizing table.");
    optimize_delta(&table_path).await;
    info!("Table optimization complete. Vacuuming table.");
    vacuum_delta(&table_path).await;
    info!("Table vacuum complete.");

    Ok(())

}
