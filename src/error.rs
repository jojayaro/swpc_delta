use thiserror::Error;

/// Comprehensive error types for SWPC Delta Lake operations
///
/// Provides rich error context and recovery strategies for debugging and monitoring.
#[derive(Error, Debug)]
pub enum SwpcDeltaError {
    /// Delta Lake table operations failed
    ///
    /// Recovery strategy: Check table permissions, schema compatibility,
    /// and ensure directory exists with proper write access.
    #[error("Delta Lake operation failed: {0}")]
    DeltaTable(#[from] deltalake::DeltaTableError),

    /// HTTP request to SWPC API failed
    ///
    /// Recovery strategy: Retry with exponential backoff, check network connectivity,
    /// and verify SWPC service status at https://services.swpc.noaa.gov/
    #[error("HTTP request failed: {0}")]
    Http(#[from] reqwest::Error),

    /// JSON parsing of API response failed
    ///
    /// Recovery strategy: Check if SWPC API format has changed, log raw response
    /// for debugging, and implement fallback parsing logic.
    #[error("JSON parsing failed: {0}")]
    Json(#[from] serde_json::Error),

    /// Date/timestamp parsing failed
    ///
    /// Recovery strategy: Check if SWPC timestamp format has changed,
    /// implement alternative date parsing formats.
    #[error("Date parsing failed: {0}")]
    DateParse(#[from] chrono::format::ParseError),

    /// File I/O operation failed
    ///
    /// Recovery strategy: Check disk space, file permissions, and directory structure.
    #[error("IO operation failed: {0}")]
    Io(#[from] std::io::Error),

    /// Data validation or transformation error
    ///
    /// Recovery strategy: Log malformed data for analysis, implement data cleansing,
    /// and continue processing valid entries.
    #[error("Data processing error: {0}")]
    DataProcessing(String),

    /// Configuration or setup error
    ///
    /// Recovery strategy: Validate configuration parameters, check environment variables,
    /// and ensure proper CLI argument handling.
    #[error("Configuration error: {0}")]
    Config(String),

    /// Apache Arrow operations failed
    ///
    /// Recovery strategy: Check schema compatibility, memory availability,
    /// and data type consistency.
    #[error("Arrow operation failed: {0}")]
    Arrow(#[from] arrow::error::ArrowError),

    /// Network connectivity or timeout issues
    ///
    /// Recovery strategy: Implement retry logic, check firewall settings,
    /// and consider using cached data during outages.
    #[error("Network operation failed: {message} (URL: {url})")]
    Network { message: String, url: String },

    /// Data validation failed with detailed context
    ///
    /// Recovery strategy: Log validation errors, skip invalid records,
    /// and implement data quality monitoring.
    #[error("Data validation failed: {field} in record {record_index} - {reason}")]
    Validation {
        field: String,
        record_index: usize,
        reason: String,
    },
}

impl SwpcDeltaError {
    /// Create a data processing error with context
    pub fn data_processing(message: impl Into<String>) -> Self {
        Self::DataProcessing(message.into())
    }

    /// Create a configuration error with context
    pub fn config(message: impl Into<String>) -> Self {
        Self::Config(message.into())
    }

    /// Create a network error with URL context
    pub fn network(message: impl Into<String>, url: impl Into<String>) -> Self {
        Self::Network {
            message: message.into(),
            url: url.into(),
        }
    }

    /// Create a validation error with detailed context
    pub fn validation(
        field: impl Into<String>,
        record_index: usize,
        reason: impl Into<String>,
    ) -> Self {
        Self::Validation {
            field: field.into(),
            record_index,
            reason: reason.into(),
        }
    }
}

pub type Result<T> = std::result::Result<T, SwpcDeltaError>;
