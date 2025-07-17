use std::sync::Arc;
use deltalake::{
    arrow::{array::{Array, Int64Array, Float64Array, StringArray}, record_batch::RecordBatch},
    operations::{
        optimize::OptimizeBuilder,
        vacuum::VacuumBuilder,
        DeltaOps
    },
    protocol::SaveMode,
    DeltaTable,
    Path,
    DeltaTableError,
};

use deltalake::kernel::{StructField, DataType, PrimitiveType};
use delta_kernel::engine::arrow_conversion::TryIntoArrow;

use datafusion::prelude::SessionContext;

use rayon::prelude::*;
use crate::swpc::SolarWind;

pub async fn create_initialized_table(table_path: &Path) -> Result<DeltaTable, DeltaTableError> {
    let ops = DeltaOps::try_from_uri(table_path).await?;
    let table = ops
        .create()
        .with_save_mode(SaveMode::ErrorIfExists)
        .with_columns(sw_columns())
        .await?;
    Ok(table)
}

pub async fn create_initialized_table_overwrite(table_path: &Path) -> Result<DeltaTable, DeltaTableError> {
    let ops = DeltaOps::try_from_uri(table_path).await?;
    let table = ops
        .create()
        .with_save_mode(SaveMode::Overwrite)
        .with_columns(sw_columns())
        .await?;
    Ok(table)
}

pub async fn optimize_delta(table_path: &Path) {
    let _ = DeltaOps::try_from_uri(table_path)
        .await
        .unwrap()
        .optimize()
        .await
        .unwrap();
}

pub fn sw_columns() -> Vec<StructField> {
    vec![
        StructField::new(
            "timestamp".to_string(),
            DataType::Primitive(PrimitiveType::Long),
            true,
        ),
        StructField::new(
            "time_tag".to_string(),
            DataType::Primitive(PrimitiveType::String),
            true,
        ),
        StructField::new(
            "speed".to_string(),
            DataType::Primitive(PrimitiveType::Double),
            true,
        ),
        StructField::new(
            "density".to_string(),
            DataType::Primitive(PrimitiveType::Double),
            true,
        ),
        StructField::new(
            "temperature".to_string(),
            DataType::Primitive(PrimitiveType::Double),
            true,
        ),
        StructField::new(
            "bt".to_string(),
            DataType::Primitive(PrimitiveType::Double),
            true,
        ),
        StructField::new(
            "bz".to_string(),
            DataType::Primitive(PrimitiveType::Double),
            true,
        )
    ]
}

pub async fn solar_wind_to_batch(table: &DeltaTable, records: Vec<SolarWind>) -> RecordBatch {
    
    let arrow_schema = table.schema().unwrap().try_into_arrow().expect("Failed to convert to arrow schema");

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
    use chrono::{Utc, Duration};

    let ctx = SessionContext::new();

    let table = match deltalake::open_table(&table_uri).await {
        Ok(table) => table,
        Err(_) => {
            // Default: 24h before today (midnight UTC)
            let now = Utc::now().date_naive().and_hms_opt(0, 0, 0).unwrap();
            let default = now - Duration::hours(24);
            return default.and_utc().timestamp();
        }
    };
    ctx.register_table("solar_wind", Arc::new(table)).unwrap();

    let batches = ctx
        .sql("SELECT COALESCE(MAX(timestamp), 1682916954) FROM solar_wind").await.unwrap()
        .collect()
        .await.unwrap();

    let max_timestamp = batches[0].column(0).as_any().downcast_ref::<Int64Array>().unwrap().value(0);

    max_timestamp
}

pub async fn vacuum_delta(table_path: &deltalake::Path) {
    let _ = DeltaOps::try_from_uri(table_path)
        .await
        .unwrap()
        .vacuum()
        .await
        .unwrap();
}
