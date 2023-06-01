use deltalake::operations::optimize::OptimizeBuilder;
use deltalake::operations::transaction;
use serde_json::Value;
use serde::{Serialize, Deserialize};
use chrono::{NaiveDateTime};
use std::collections::HashMap;
use deltalake::action::*;
use deltalake::arrow::array::*;
use deltalake::arrow::record_batch::RecordBatch;
use deltalake::writer::{DeltaWriter, RecordBatchWriter};
use deltalake::*;
use datafusion::prelude::SessionContext;
use object_store::path::Path;
use std::sync::Arc;
use rayon::prelude::*;


#[derive(Debug, Serialize, Deserialize)]
struct SolarWind {
    timestamp: i64,
    time_tag: String,
    speed: f64,
    density: f64,
    temperature: f64,
    bt: f64,
    bz: f64
}

async fn solar_wind_payload() -> Vec<Value> {
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

fn payload_to_solarwind(response: Vec<Value>) -> Vec<SolarWind> {
      
    response
        .par_iter()
        .map(|x| SolarWind {
            timestamp: NaiveDateTime::parse_from_str(&x[0].to_string().replace("\"",""), "%Y-%m-%d %H:%M:%S%.3f").unwrap().timestamp(),
            time_tag: x[0].as_str().unwrap().to_string(),
            speed: x[1].to_string().replace("\"","").parse::<f64>().unwrap_or(0.0),
            density: x[2].to_string().replace("\"","").parse::<f64>().unwrap_or(0.0),
            temperature: x[3].to_string().replace("\"","").parse::<f64>().unwrap_or(0.0),
            bt: x[7].to_string().replace("\"","").parse::<f64>().unwrap_or(0.0),
            bz: x[6].to_string().replace("\"","").parse::<f64>().unwrap_or(0.0)
        }).collect::<Vec<SolarWind>>()
}

async fn filtered_solar_wind_data(timestamp: i64, solar_wind: Vec<SolarWind>) -> Vec<SolarWind> {

    let filtered_solar_wind = solar_wind
        .into_par_iter()
        .filter(|x| x.timestamp > timestamp)
        .collect::<Vec<SolarWind>>();

    filtered_solar_wind

}

fn sw_columns() -> Vec<SchemaField> {
        vec![
            SchemaField::new(
                "timestamp".to_string()
                , SchemaDataType::primitive("long".to_string())
                , true
                , HashMap::new()
            ),
            SchemaField::new(
                "time_tag".to_string()
                , SchemaDataType::primitive("string".to_string())
                , true
                , HashMap::new()
            ),
            SchemaField::new(
                "speed".to_string()
                , SchemaDataType::primitive("double".to_string())
                , true
                , HashMap::new()
            ),
            SchemaField::new(
                "density".to_string()
                , SchemaDataType::primitive("double".to_string())
                , true
                , HashMap::new()
            ),
            SchemaField::new(
                "temperature".to_string()
                , SchemaDataType::primitive("double".to_string())
                , true
                , HashMap::new()
            ),
            SchemaField::new(
                "bt".to_string()
                , SchemaDataType::primitive("double".to_string())
                , true
                , HashMap::new()
            ),
            SchemaField::new(
                "bz".to_string()
                , SchemaDataType::primitive("double".to_string())
                , true
                , HashMap::new()
            )
        ]
}

async fn solar_wind_to_batch(table: &DeltaTable, records: Vec<SolarWind>) -> RecordBatch {
        
    let metadata = table
        .get_metadata()
        .expect("Failed to get metadata");
    
    let arrow_schema = <deltalake::arrow::datatypes::Schema as TryFrom<&Schema>>::try_from(
        &metadata.schema.clone(),
    )
    .expect("Failed to convert to arrow schema");
    
    let arrow_schema_ref = Arc::new(arrow_schema);

    let solar_wind = records;

    let arrow_array: Vec<Arc<dyn Array>> = vec![
        Arc::new(Int64Array::from(solar_wind.par_iter().map(|x| x.timestamp.clone()).collect::<Vec<i64>>())),
        Arc::new(StringArray::from(solar_wind.par_iter().map(|x| x.time_tag.clone()).collect::<Vec<String>>())),
        Arc::new(Float64Array::from(solar_wind.par_iter().map(|x| x.speed.clone()).collect::<Vec<f64>>())),
        Arc::new(Float64Array::from(solar_wind.par_iter().map(|x| x.density.clone()).collect::<Vec<f64>>())),
        Arc::new(Float64Array::from(solar_wind.par_iter().map(|x| x.temperature.clone()).collect::<Vec<f64>>())),
        Arc::new(Float64Array::from(solar_wind.par_iter().map(|x| x.bt.clone()).collect::<Vec<f64>>())),
        Arc::new(Float64Array::from(solar_wind.par_iter().map(|x| x.bz.clone()).collect::<Vec<f64>>())),
    ];
    
    RecordBatch::try_new(arrow_schema_ref, arrow_array).expect("Failed to create RecordBatch")
}

async fn max_solar_wind_timestamp(table_uri: String) -> i64 {
    let ctx = SessionContext::new();
    let table = deltalake::open_table(table_uri)
        .await
        .unwrap();
    ctx.register_table("solar_wind", Arc::new(table)).unwrap();
  
    let batches = ctx
        .sql("SELECT COALESCE(MAX(timestamp), 1682916954) FROM solar_wind").await.unwrap()
        .collect()
        .await.unwrap();

    let max_timestamp = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);

    max_timestamp
}

async fn create_initialized_table(table_path: &Path) -> DeltaTable {
    
    DeltaOps::try_from_uri(table_path)
        .await
        .unwrap()
        .create()
        .with_save_mode(SaveMode::Append)
        .with_columns(sw_columns())
        .await
        .unwrap()
}

async fn optimize_delta(table_path: &Path) {
    let mut table = deltalake::open_table(table_path).await.unwrap();
    let (table, metrics) = OptimizeBuilder::new(table.object_store(), table.state).await.unwrap();
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {

    //let adls = env::var("ADLS").unwrap();

    let table_uri = "/solar_wind/".to_string(); //std::env::var("TABLE_URI")?;

    let table_path = Path::from(table_uri.as_ref());

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

        optimize_delta(&table_path).await;
    
        writer.write(batch).await?;
    
        writer
            .flush_and_commit(&mut table)
            .await
            .expect("Failed to flush write");

    }

    Ok(())

}


    