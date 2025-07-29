use deltalake::{
    arrow::{
        array::{Array, Float64Array, Int32Array, Int64Array, StringArray},
        record_batch::RecordBatch,
    },
    operations::DeltaOps,
    protocol::SaveMode,
    DeltaTable, DeltaTableError, Path,
};
use std::sync::Arc;

use crate::swpc::Magnetometer;
use delta_kernel::engine::arrow_conversion::TryIntoArrow;
use deltalake::kernel::{DataType, PrimitiveType, StructField};

use datafusion::prelude::SessionContext;

use crate::swpc::SolarWind;
use rayon::prelude::*;

pub async fn create_initialized_table_with_columns(
    table_path: &Path,
    columns: Vec<StructField>,
) -> std::result::Result<DeltaTable, DeltaTableError> {
    // Ensure table directory exists
    std::fs::create_dir_all(table_path.as_ref()).map_err(|e| {
        DeltaTableError::Generic(format!(
            "Failed to create directory {}: {}",
            table_path.as_ref(),
            e
        ))
    })?;

    let ops = DeltaOps::try_from_uri(table_path).await?;
    let table = ops
        .create()
        .with_save_mode(SaveMode::ErrorIfExists)
        .with_columns(columns)
        .await?;
    Ok(table)
}

pub async fn create_initialized_table_with_columns_overwrite(
    table_path: &Path,
    columns: Vec<StructField>,
) -> std::result::Result<DeltaTable, DeltaTableError> {
    std::fs::create_dir_all(table_path.as_ref()).map_err(|e| {
        DeltaTableError::Generic(format!(
            "Failed to create directory {}: {}",
            table_path.as_ref(),
            e
        ))
    })?;

    let ops = DeltaOps::try_from_uri(table_path).await?;
    let table = ops
        .create()
        .with_save_mode(SaveMode::Overwrite)
        .with_columns(columns)
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
        ),
    ]
}

pub fn magnetometer_columns() -> Vec<StructField> {
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
            "satellite".to_string(),
            DataType::Primitive(PrimitiveType::Long),
            true,
        ),
        StructField::new(
            "he".to_string(),
            DataType::Primitive(PrimitiveType::Double),
            true,
        ),
        StructField::new(
            "hp".to_string(),
            DataType::Primitive(PrimitiveType::Double),
            true,
        ),
        StructField::new(
            "hn".to_string(),
            DataType::Primitive(PrimitiveType::Double),
            true,
        ),
        StructField::new(
            "total".to_string(),
            DataType::Primitive(PrimitiveType::Double),
            true,
        ),
        StructField::new(
            "arcjet_flag".to_string(),
            DataType::Primitive(PrimitiveType::Integer),
            true,
        ),
    ]
}

pub async fn create_initialized_table_magnetometer(
    table_path: &Path,
) -> std::result::Result<DeltaTable, DeltaTableError> {
    // Ensure table directory exists
    std::fs::create_dir_all(table_path.as_ref()).map_err(|e| {
        DeltaTableError::Generic(format!(
            "Failed to create directory {}: {}",
            table_path.as_ref(),
            e
        ))
    })?;

    let ops = DeltaOps::try_from_uri(table_path).await?;
    let table = ops
        .create()
        .with_save_mode(SaveMode::ErrorIfExists)
        .with_columns(magnetometer_columns())
        .await?;
    Ok(table)
}

pub async fn magnetometer_to_batch(table: &DeltaTable, records: Vec<Magnetometer>) -> RecordBatch {
    let arrow_schema = table
        .schema()
        .unwrap()
        .try_into_arrow()
        .expect("Failed to convert to arrow schema");
    let arrow_schema_ref = Arc::new(arrow_schema);

    let magnetometer = records;

    let arrow_array: Vec<Arc<dyn Array>> = vec![
        Arc::new(Int64Array::from(
            magnetometer
                .par_iter()
                .map(|x| x.timestamp)
                .collect::<Vec<i64>>(),
        )),
        Arc::new(StringArray::from(
            magnetometer
                .par_iter()
                .map(|x| x.time_tag.clone())
                .collect::<Vec<String>>(),
        )),
        Arc::new(Int64Array::from(
            magnetometer
                .par_iter()
                .map(|x| x.satellite as i64)
                .collect::<Vec<i64>>(),
        )),
        Arc::new(Float64Array::from(
            magnetometer.par_iter().map(|x| x.he).collect::<Vec<f64>>(),
        )),
        Arc::new(Float64Array::from(
            magnetometer.par_iter().map(|x| x.hp).collect::<Vec<f64>>(),
        )),
        Arc::new(Float64Array::from(
            magnetometer.par_iter().map(|x| x.hn).collect::<Vec<f64>>(),
        )),
        Arc::new(Float64Array::from(
            magnetometer
                .par_iter()
                .map(|x| x.total)
                .collect::<Vec<f64>>(),
        )),
        Arc::new(Int32Array::from(
            magnetometer
                .par_iter()
                .map(|x| if x.arcjet_flag { 1i32 } else { 0i32 })
                .collect::<Vec<i32>>(),
        )),
    ];

    RecordBatch::try_new(arrow_schema_ref, arrow_array).expect("Failed to create RecordBatch")
}

pub async fn max_magnetometer_timestamp(table_uri: String) -> i64 {
    use chrono::{Duration, Utc};

    let ctx = SessionContext::new();

    let table = match deltalake::open_table(&table_uri).await {
        Ok(table) => table,
        Err(_) => {
            // Default: 6h before today (midnight UTC)
            let now = Utc::now().date_naive().and_hms_opt(0, 0, 0).unwrap();
            let default = now - Duration::hours(6);
            return default.and_utc().timestamp();
        }
    };
    ctx.register_table("magnetometer", Arc::new(table)).unwrap();

    let batches = ctx
        .sql("SELECT COALESCE(MAX(timestamp), 1682916954) FROM magnetometer")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let max_timestamp = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);

    max_timestamp
}

pub async fn solar_wind_to_batch(table: &DeltaTable, records: Vec<SolarWind>) -> RecordBatch {
    let arrow_schema = table
        .schema()
        .unwrap()
        .try_into_arrow()
        .expect("Failed to convert to arrow schema");
    let arrow_schema_ref = Arc::new(arrow_schema);
    let solar_wind = records;

    let arrow_array: Vec<Arc<dyn Array>> = vec![
        Arc::new(Int64Array::from(
            solar_wind
                .par_iter()
                .map(|x| x.timestamp)
                .collect::<Vec<i64>>(),
        )),
        Arc::new(StringArray::from(
            solar_wind
                .par_iter()
                .map(|x| x.time_tag.clone())
                .collect::<Vec<String>>(),
        )),
        Arc::new(Float64Array::from(
            solar_wind.par_iter().map(|x| x.speed).collect::<Vec<f64>>(),
        )),
        Arc::new(Float64Array::from(
            solar_wind
                .par_iter()
                .map(|x| x.density)
                .collect::<Vec<f64>>(),
        )),
        Arc::new(Float64Array::from(
            solar_wind
                .par_iter()
                .map(|x| x.temperature)
                .collect::<Vec<f64>>(),
        )),
        Arc::new(Float64Array::from(
            solar_wind.par_iter().map(|x| x.bt).collect::<Vec<f64>>(),
        )),
        Arc::new(Float64Array::from(
            solar_wind.par_iter().map(|x| x.bz).collect::<Vec<f64>>(),
        )),
    ];

    RecordBatch::try_new(arrow_schema_ref, arrow_array).expect("Failed to create RecordBatch")
}

pub async fn max_solar_wind_timestamp(table_uri: String) -> i64 {
    use chrono::{Duration, Utc};

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
        .sql("SELECT COALESCE(MAX(timestamp), 1682916954) FROM solar_wind")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();

    let max_timestamp = batches[0]
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);

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
