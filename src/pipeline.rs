use deltalake::{
    writer::{DeltaWriter, RecordBatchWriter},
    DeltaTableError,
};
use log::{error, info};

use crate::{
    cli::Args,
    delta::{
        create_initialized_table_with_columns, magnetometer_columns, magnetometer_to_batch,
        max_magnetometer_timestamp, max_solar_wind_timestamp, optimize_delta, solar_wind_to_batch,
        sw_columns, vacuum_delta,
    },
    error::SwpcDeltaError,
    swpc::{
        filtered_magnetometer_data, filtered_solar_wind_data, magnetometer_payload,
        payload_to_magnetometer, payload_to_solarwind, solar_wind_payload, Magnetometer, SolarWind,
    },
};

/// Common data ingestion pipeline for both solar wind and magnetometer data
pub async fn run_pipeline(args: Args) -> Result<(), SwpcDeltaError> {
    env_logger::init();

    // Process solar wind data
    let mut solar_wind_table = initialize_or_open_solar_wind_table(&args.solar_wind_path).await?;
    process_solar_wind_data(&mut solar_wind_table, &args.solar_wind_path).await?;

    // Process magnetometer data
    let mut magnetometer_table =
        initialize_or_open_magnetometer_table(&args.magnetometer_path).await?;
    process_magnetometer_data(&mut magnetometer_table, &args.magnetometer_path).await?;

    // Optimize tables if not skipped
    if !args.skip_optimization {
        optimize_tables(&args.solar_wind_path, &args.magnetometer_path).await;
    } else {
        info!("Skipping optimization and vacuum operations.");
    }

    Ok(())
}

/// Initialize or open an existing solar wind Delta Lake table
async fn initialize_or_open_solar_wind_table(
    table_path: &str,
) -> Result<deltalake::DeltaTable, SwpcDeltaError> {
    info!(
        "Attempting to open solar wind Delta Lake table at: {}",
        table_path
    );

    let table_path_obj = deltalake::Path::from(table_path);

    let maybe_table = deltalake::open_table(table_path).await;
    match maybe_table {
        Ok(table) => {
            info!("Successfully opened existing solar wind Delta Lake table.");
            Ok(table)
        }
        Err(DeltaTableError::NotATable(_)) => {
            info!("Solar wind Delta Lake table not found. Creating a new one.");
            create_initialized_table_with_columns(&table_path_obj, sw_columns())
                .await
                .map_err(SwpcDeltaError::DeltaTable)
        }
        Err(err) => {
            error!("Failed to open solar wind Delta Lake table: {}", err);
            Err(SwpcDeltaError::DeltaTable(err))
        }
    }
}

/// Initialize or open an existing magnetometer Delta Lake table
async fn initialize_or_open_magnetometer_table(
    table_path: &str,
) -> Result<deltalake::DeltaTable, SwpcDeltaError> {
    info!(
        "Attempting to open magnetometer Delta Lake table at: {}",
        table_path
    );

    let table_path_obj = deltalake::Path::from(table_path);

    let maybe_table = deltalake::open_table(table_path).await;
    match maybe_table {
        Ok(table) => {
            info!("Successfully opened existing magnetometer Delta Lake table.");
            Ok(table)
        }
        Err(DeltaTableError::NotATable(_)) => {
            info!("Magnetometer Delta Lake table not found. Creating a new one.");
            create_initialized_table_with_columns(&table_path_obj, magnetometer_columns())
                .await
                .map_err(SwpcDeltaError::DeltaTable)
        }
        Err(err) => {
            error!("Failed to open magnetometer Delta Lake table: {}", err);
            Err(SwpcDeltaError::DeltaTable(err))
        }
    }
}

/// Process and ingest solar wind data
async fn process_solar_wind_data(
    table: &mut deltalake::DeltaTable,
    table_path: &str,
) -> Result<(), SwpcDeltaError> {
    info!("Fetching max solar wind timestamp.");
    let timestamp = max_solar_wind_timestamp(table_path.to_string()).await;
    info!("Max solar wind timestamp: {}", timestamp);

    info!("Fetching solar wind payload.");
    let solar_wind_payload_data = solar_wind_payload().await?;

    info!("Filtering solar wind data.");
    let solar_wind =
        filtered_solar_wind_data(timestamp, payload_to_solarwind(solar_wind_payload_data)?).await;

    ingest_solar_wind_data(table, solar_wind).await
}

/// Process and ingest magnetometer data
async fn process_magnetometer_data(
    table: &mut deltalake::DeltaTable,
    table_path: &str,
) -> Result<(), SwpcDeltaError> {
    info!("Fetching max magnetometer timestamp.");
    let magnetometer_timestamp = max_magnetometer_timestamp(table_path.to_string()).await;
    info!("Max magnetometer timestamp: {}", magnetometer_timestamp);

    info!("Fetching magnetometer payload.");
    let magnetometer_payload_data = magnetometer_payload().await?;

    info!("Filtering magnetometer data.");
    let magnetometer = filtered_magnetometer_data(
        magnetometer_timestamp,
        payload_to_magnetometer(magnetometer_payload_data)?,
    )
    .await;

    ingest_magnetometer_data(table, magnetometer).await
}

/// Ingest solar wind data into Delta Lake
async fn ingest_solar_wind_data(
    table: &mut deltalake::DeltaTable,
    data: Vec<SolarWind>,
) -> Result<(), SwpcDeltaError> {
    if !data.is_empty() {
        info!(
            "{} new solar wind records found. Ingesting data.",
            data.len()
        );

        let batch = solar_wind_to_batch(table, data).await;
        let mut writer = RecordBatchWriter::for_table(table)?;
        writer.write(batch).await?;
        writer.flush_and_commit(table).await?;

        info!("Solar wind data ingestion complete.");
    } else {
        info!("No new solar wind records to ingest.");
    }

    Ok(())
}

/// Ingest magnetometer data into Delta Lake
async fn ingest_magnetometer_data(
    table: &mut deltalake::DeltaTable,
    data: Vec<Magnetometer>,
) -> Result<(), SwpcDeltaError> {
    if !data.is_empty() {
        info!(
            "{} new magnetometer records found. Ingesting data.",
            data.len()
        );

        let batch = magnetometer_to_batch(table, data).await;
        let mut writer = RecordBatchWriter::for_table(table)?;
        writer.write(batch).await?;
        writer.flush_and_commit(table).await?;

        info!("Magnetometer data ingestion complete.");
    } else {
        info!("No new magnetometer records to ingest.");
    }

    Ok(())
}

/// Optimize both solar wind and magnetometer tables
async fn optimize_tables(solar_wind_path: &str, magnetometer_path: &str) {
    let solar_wind_table_path = deltalake::Path::from(solar_wind_path);
    let magnetometer_table_path = deltalake::Path::from(magnetometer_path);

    info!("Optimizing solar wind table.");
    optimize_delta(&solar_wind_table_path).await;
    info!("Solar wind table optimization complete.");

    info!("Vacuuming solar wind table.");
    vacuum_delta(&solar_wind_table_path).await;
    info!("Solar wind table vacuum complete.");

    info!("Optimizing magnetometer table.");
    optimize_delta(&magnetometer_table_path).await;
    info!("Magnetometer table optimization complete.");

    info!("Vacuuming magnetometer table.");
    vacuum_delta(&magnetometer_table_path).await;
    info!("Magnetometer table vacuum complete.");
}
