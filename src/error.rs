use thiserror::Error;

#[derive(Error, Debug)]
pub enum SwpcDeltaError {
    #[error("Delta Lake operation failed: {0}")]
    DeltaTable(#[from] deltalake::DeltaTableError),

    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    #[error("JSON parsing failed: {0}")]
    Json(#[from] serde_json::Error),

    #[error("Date parsing failed: {0}")]
    DateParse(#[from] chrono::format::ParseError),

    #[error("IO operation failed: {0}")]
    Io(#[from] std::io::Error),

    #[error("Data processing error: {0}")]
    DataProcessing(String),

    #[error("Configuration error: {0}")]
    Config(String),

    #[error("Arrow operation failed: {0}")]
    Arrow(#[from] arrow::error::ArrowError),
}

pub type Result<T> = std::result::Result<T, SwpcDeltaError>;
