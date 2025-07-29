use clap::Parser;
use deltalake::{
    writer::{DeltaWriter, RecordBatchWriter},
    DeltaTableError,
};
use log::{error, info};
use swpc_delta::{
    delta::{
        create_initialized_table_with_columns, magnetometer_to_batch, max_magnetometer_timestamp,
        max_solar_wind_timestamp, optimize_delta, solar_wind_to_batch, vacuum_delta, sw_columns,
        magnetometer_columns,
    },
    error::SwpcDeltaError,
    swpc::{
        filtered_magnetometer_data, filtered_solar_wind_data, magnetometer_payload,
        payload_to_magnetometer, payload_to_solarwind, solar_wind_payload,
    },
};

#[derive(Parser, Debug)]
#[command(name = "swpc_delta")]
#[command(about = "SWPC Solar Wind and Magnetometer data ingestion to Delta Lake")]
struct Args {
    /// Solar wind Delta Lake table directory path
    #[clap(
        long,
        default_value = "./solar_wind_table",
        help = "Path to solar wind Delta Lake table directory"
    )]
    solar_wind_path: String,

    /// Magnetometer Delta Lake table directory path
    #[clap(
        long,
        default_value = "./magnetometer_table",
        help = "Path to magnetometer Delta Lake table directory"
    )]
    magnetometer_path: String,

    /// Skip optimization and vacuum for faster ingestion
    #[clap(long, help = "Skip table optimization and vacuum operations")]
    skip_optimization: bool,
}

#[tokio::main]
async fn main() -> Result<(), SwpcDeltaError> {
    let args = Args::parse();
    env_logger::init();

    let table_uri = args.solar_wind_path.clone();

    info!(
        "Attempting to open solar wind Delta Lake table at: {}",
        table_uri
    );
    let table_path = deltalake::Path::from(table_uri.as_str());

    let maybe_table = deltalake::open_table(&table_uri).await;
    let mut table = match maybe_table {
        Ok(table) => {
            info!("Successfully opened existing Delta Lake table.");
            table
        }
        Err(DeltaTableError::NotATable(_)) => {
            info!("Delta Lake table not found. Creating a new one.");
            create_initialized_table_with_columns(&table_path, sw_columns()).await?
        }
        Err(err) => {
            error!("Failed to open Delta Lake table: {}", err);
            return Err(SwpcDeltaError::DeltaTable(err));
        }
    };

    let magnetometer_table_path = deltalake::Path::from(args.magnetometer_path.as_str());
    let maybe_magnetometer_table = deltalake::open_table(&args.magnetometer_path).await;
    let mut magnetometer_table = match maybe_magnetometer_table {
        Ok(table) => {
            info!("Successfully opened existing magnetometer Delta Lake table.");
            table
        }
        Err(DeltaTableError::NotATable(_)) => {
            info!("Magnetometer Delta Lake table not found. Creating a new one.");
            create_initialized_table_with_columns(&magnetometer_table_path, magnetometer_columns()).await?
        }
        Err(err) => {
            error!("Failed to open magnetometer Delta Lake table: {}", err);
            return Err(SwpcDeltaError::DeltaTable(err));
        }
    };

    info!("Fetching max solar wind timestamp.");
    let timestamp = max_solar_wind_timestamp(args.solar_wind_path.clone()).await;
    info!("Max solar wind timestamp: {}", timestamp);

    info!("Fetching solar wind payload.");
    let solar_wind_payload_data = solar_wind_payload().await?;
    info!("Filtering solar wind data.");
    let solar_wind =
        filtered_solar_wind_data(timestamp, payload_to_solarwind(solar_wind_payload_data)?).await;

    if !solar_wind.is_empty() {
        info!(
            "{} new solar wind records found. Ingesting data.",
            solar_wind.len()
        );
        let batch = solar_wind_to_batch(&table, solar_wind).await;

        let mut writer = RecordBatchWriter::for_table(&table)?;
        writer.write(batch).await?;
        writer.flush_and_commit(&mut table).await?;

        info!("Solar wind data ingestion complete.");
    } else {
        info!("No new solar wind records to ingest.");
    }

    info!("Fetching max magnetometer timestamp.");
    let magnetometer_timestamp = max_magnetometer_timestamp(args.magnetometer_path.clone()).await;
    info!("Max magnetometer timestamp: {}", magnetometer_timestamp);

    info!("Fetching magnetometer payload.");
    let magnetometer_payload_data = magnetometer_payload().await?;
    info!("Filtering magnetometer data.");
    let magnetometer = filtered_magnetometer_data(
        magnetometer_timestamp,
        payload_to_magnetometer(magnetometer_payload_data)?,
    )
    .await;

    if !magnetometer.is_empty() {
        info!(
            "{} new magnetometer records found. Ingesting data.",
            magnetometer.len()
        );
        let batch = magnetometer_to_batch(&magnetometer_table, magnetometer).await;

        let mut writer = RecordBatchWriter::for_table(&magnetometer_table)?;
        writer.write(batch).await?;
        writer.flush_and_commit(&mut magnetometer_table).await?;

        info!("Magnetometer data ingestion complete.");
    } else {
        info!("No new magnetometer records to ingest.");
    }

    if !args.skip_optimization {
        info!("Optimizing solar wind table.");
        optimize_delta(&table_path).await;
        info!("Solar wind table optimization complete.");

        info!("Vacuuming solar wind table.");
        vacuum_delta(&table_path).await;
        info!("Solar wind table vacuum complete.");

        info!("Optimizing magnetometer table.");
        optimize_delta(&magnetometer_table_path).await;
        info!("Magnetometer table optimization complete.");

        info!("Vacuuming magnetometer table.");
        vacuum_delta(&magnetometer_table_path).await;
        info!("Magnetometer table vacuum complete.");
    } else {
        info!("Skipping optimization and vacuum operations.");
    }

    Ok(())
}
