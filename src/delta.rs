use std::{collections::HashMap, sync::Arc};
use deltalake::{Path, DeltaTable, DeltaOps, action::SaveMode, operations::{optimize::OptimizeBuilder, vacuum::VacuumBuilder}, SchemaField, SchemaDataType, arrow::{record_batch::RecordBatch, array::{Int64Array, StringArray, Float64Array, Array}}, Schema, datafusion::prelude::SessionContext};
use rayon::prelude::*;
use crate::swpc::SolarWind;

pub async fn create_initialized_table(table_path: &Path) -> DeltaTable {
    
    DeltaOps::try_from_uri(table_path)
        .await
        .unwrap()
        .create()
        .with_save_mode(SaveMode::Append)
        .with_columns(sw_columns())
        .await
        .unwrap()
}

pub async fn optimize_delta(table_path: &Path) {

    let mut table = deltalake::open_table(table_path).await.unwrap();
    let (table, metrics) = OptimizeBuilder::new(table.object_store(), table.state).await.unwrap();

}

pub fn sw_columns() -> Vec<SchemaField> {
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

pub async fn solar_wind_to_batch(table: &DeltaTable, records: Vec<SolarWind>) -> RecordBatch {
    
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

pub async fn max_solar_wind_timestamp(table_uri: String) -> i64 {

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

pub async fn vacuum_delta(table_path: &Path) {
    
        let mut table = deltalake::open_table(table_path).await.unwrap();
        let (table, metrics) = VacuumBuilder::new(table.object_store(), table.state).await.unwrap();
    
}