# deltalake Upgrade and Optimization

This document summarizes the changes made to upgrade the `deltalake` package to version `0.27.0` and optimize the data ingestion process.

## Changes Made:

- **`Cargo.toml` Updates:**
  - Upgraded `deltalake` to `0.27.0`.
  - Removed the `rustc-serialize` feature from `chrono` dependency due to incompatibility with newer `chrono` versions.
  - Added `datafusion = "48.0.1"` and `delta_kernel = "0.13.0"` as new dependencies to support `deltalake` 0.27.0.

- **`src/delta.rs` Refactoring:**
  - Updated import paths for `deltalake` modules, including `arrow` arrays, `operations`, `protocol`, `Path`, `StructField`, `DataType`, and `PrimitiveType`.
  - Changed `table.get_metadata()` to `table.metadata()` as per the new API.
  - Modified `OptimizeBuilder::new()` and `VacuumBuilder::new()` calls to correctly use `table.log_store()` and `table.state.unwrap()`.
  - Adjusted `sw_columns()` function to use `StructField::new()` with the correct number of arguments and `DataType::Primitive` enum variants.
  - Updated schema conversion in `solar_wind_to_batch()` to use `table.schema().unwrap().try_into_arrow()`.
  - Marked unused variables with `_` to suppress warnings.

- **`src/main.rs` Updates:**
  - Removed explicit `Path` type annotation for `table_path` to resolve type conflicts.
  - Removed unused `Path` import.

- **`src/swpc.rs` Fixes:**
  - Corrected string replacement logic for parsing `f64` values by properly escaping double quotes.
  - Adjusted the timestamp format string in `NaiveDateTime::parse_from_str` to include `Z` for UTC.

## Current Status:

- The project now builds successfully with `deltalake` 0.27.0.

## Next Steps:

- Add comprehensive unit tests for the updated functionalities.
- Implement proper logging and error handling across the application.
- Verify the end-to-end data ingestion, vacuum, and optimize processes.