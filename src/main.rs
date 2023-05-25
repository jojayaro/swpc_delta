use serde_json::Value;
use reqwest::Client;
use serde::{Serialize, Deserialize};
use chrono::{DateTime, Utc, TimeZone, NaiveDateTime};
use std::collections::HashMap;
use std::env;
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
        .iter()
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

    let solar_wind = payload_to_solarwind(solar_wind_payload().await);

    let filtered_solar_wind = solar_wind
        .into_iter()
        .filter(|x| x.timestamp > timestamp)
        .collect::<Vec<SolarWind>>();

    filtered_solar_wind

}

fn solar_wind_schema() -> Schema {
    Schema::new(
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
    )
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

    let table_uri = "./solar_wind".to_string();

    let solar_wind = filtered_solar_wind_data(max_solar_wind_timestamp(table_uri).await, payload_to_solarwind(solar_wind_payload().await)).await;

    let arrow_array: Vec<Arc<dyn Array>> = vec![
        Arc::new(Int64Array::from(solar_wind.iter().map(|x| x.timestamp.clone()).collect::<Vec<i64>>())),
        Arc::new(StringArray::from(solar_wind.iter().map(|x| x.time_tag.clone()).collect::<Vec<String>>())),
        Arc::new(Float64Array::from(solar_wind.iter().map(|x| x.speed.clone()).collect::<Vec<f64>>())),
        Arc::new(Float64Array::from(solar_wind.iter().map(|x| x.density.clone()).collect::<Vec<f64>>())),
        Arc::new(Float64Array::from(solar_wind.iter().map(|x| x.temperature.clone()).collect::<Vec<f64>>())),
        Arc::new(Float64Array::from(solar_wind.iter().map(|x| x.bt.clone()).collect::<Vec<f64>>())),
        Arc::new(Float64Array::from(solar_wind.iter().map(|x| x.bz.clone()).collect::<Vec<f64>>())),
    ];
    
    RecordBatch::try_new(arrow_schema_ref, arrow_array).expect("Failed to create RecordBatch")
}

async fn max_solar_wind_timestamp(table_uri: String) -> i64 {
    let mut ctx = SessionContext::new();
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
    
    let mut table = DeltaTableBuilder::from_uri(table_path).build().unwrap();
    
    let table_schema = solar_wind_schema();
    
    let mut commit_info = serde_json::Map::<String, serde_json::Value>::new();
    commit_info.insert(
        "operation".to_string(),
        serde_json::Value::String("CREATE TABLE".to_string()),
    );
    commit_info.insert(
        "userName".to_string(),
        serde_json::Value::String("sidefxs".to_string()),
    );

    let protocol = Protocol {
        min_reader_version: 1,
        min_writer_version: 1,
    };

    let metadata = DeltaTableMetaData::new(None, None, None, table_schema, vec![], HashMap::new());

    table
        .create(metadata, protocol, Some(commit_info), None)
        .await
        .unwrap();

    table
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

    let mut writer =
        RecordBatchWriter::for_table(&table).expect("Failed to make RecordBatchWriter");

    let timestamp = max_solar_wind_timestamp("./solar_wind".to_string()).await;

    let solar_wind = filtered_solar_wind_data(timestamp, payload_to_solarwind(solar_wind_payload().await)).await;

    let batch = solar_wind_to_batch(&table, solar_wind).await;

    writer.write(batch).await?;

    let actions: Vec<action::Action> = writer.flush().await?.iter()
        .map(|add| Action::add(add.clone()))
        .collect();
    let mut transaction = table.create_transaction(Some(DeltaTransactionOptions::new(/*max_retry_attempts=*/3)));
    
    transaction.add_actions(actions);
    
    transaction.commit(Some(DeltaOperation::Write {
        mode: SaveMode::Append,
        partition_by: None,
        predicate: None,
    }), None).await?;

    // let adds = writer
    //     .flush_and_commit(&mut table)
    //     .await
    //     .expect("Failed to flush write");

    Ok(())

}


    