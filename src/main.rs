use deltalake::{DeltaTableError, writer::{RecordBatchWriter, DeltaWriter}};
use swpc_delta::{
    delta::{create_initialized_table, max_solar_wind_timestamp, solar_wind_to_batch, optimize_delta, vacuum_delta}, 
    swpc::{filtered_solar_wind_data, payload_to_solarwind, solar_wind_payload}
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    //let adls = env::var("ADLS").unwrap();

    let table_uri = "/solar_wind/".to_string(); //std::env::var("TABLE_URI")?;

    let table_path = deltalake::Path::from(table_uri.as_ref());

    let maybe_table = deltalake::open_table(&table_path).await;
    let mut table = match maybe_table {
        Ok(table) => table,
        Err(DeltaTableError::NotATable(_)) => {
            create_initialized_table(&table_path).await
        }
        Err(err) => Err(err).unwrap(),
    };

    let mut writer = RecordBatchWriter::for_table(&table).expect("Failed to make RecordBatchWriter");

    let timestamp = max_solar_wind_timestamp("./solar_wind".to_string()).await;

    let solar_wind = filtered_solar_wind_data(timestamp, payload_to_solarwind(solar_wind_payload().await)).await;

    if solar_wind.len() > 0 {
        
        let batch = solar_wind_to_batch(&table, solar_wind).await;

        writer.write(batch).await?;
    
        writer
            .flush_and_commit(&mut table)
            .await
            .expect("Failed to flush write");

        optimize_delta(&table_path).await;

        vacuum_delta(&table_path).await;

    }

    Ok(())

}