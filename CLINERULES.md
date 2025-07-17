# Clinerules for swpc_delta

## Code Style
- Use Rust 2021 edition.
- Prefer `DeltaOps` for all Delta Lake table operations (create, optimize, vacuum, etc.).
- Handle errors using `Result` and propagate with `?` where possible. Avoid using `.unwrap()` and `.expect()` in production/library code.
- Use `SaveMode::ErrorIfExists` for production table creation, `SaveMode::Overwrite` for tests.
- Use `chrono` for all date/time handling; avoid deprecated methods.
- Use `rayon` for parallel data processing only when data size justifies the overhead; benchmark if unsure.
- Prefer serde-based direct struct deserialization for JSON parsing when possible.
- Use `&str` for function parameters unless ownership is required.
- Use `Option` for nullable or missing data fields instead of defaulting to zero or empty values.
- Place schema definitions and constants in a dedicated module or section for clarity.

## Testing
- All new features and bugfixes must include integration tests.
- Tests should clean up any files or directories they create.
- Use the test-only `create_initialized_table_overwrite` for table creation in tests.

## Error Handling
- If a Delta table does not exist, default to a timestamp 24h before the current day.
- Avoid panics in production code; handle errors gracefully and log them.
- Propagate errors to the top-level and handle/report them in main.rs.
- All network, parsing, and IO errors must be handled without panics.

## Contributions
- All code changes must be formatted with `cargo fmt`.
- Run `cargo test` before submitting changes to ensure all tests pass.
- Commit messages should be clear and reference the main change (e.g., "Refactor: use DeltaOps for all table operations").

## Documentation
- Public functions must have doc comments.
- Add unit tests for all data parsing and filtering logic.
- Use structured logging (e.g., `tracing`) for async contexts if advanced logging is needed.
- Update this file with any new conventions or best practices.
