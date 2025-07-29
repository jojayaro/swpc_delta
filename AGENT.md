# SWPC Solar Wind Delta Lake - Agent Guidelines

## Commands
- **Build**: `cargo build` / `cargo build --release`
- **Test**: `cargo test` / `cargo test --lib` (for single test: `cargo test test_name`)
- **Lint**: `cargo clippy`
- **Format**: `cargo fmt`
- **Run**: `./target/release/swpc_delta` or `cargo run`
- **Usage**: `swpc_delta --solar-wind-path ./solar_wind_table --magnetometer-path ./magnetometer_table --skip-optimization`

## Architecture
- **Main module**: `src/main.rs` - CLI entry point with clap args for both pipelines
- **Library modules**: `src/lib.rs`, `src/delta.rs` (Delta Lake ops), `src/swpc.rs` (API client), `src/error.rs` (error types)
- **Data pipeline**: SWPC API → JSON parsing → Delta Lake tables (solar_wind + magnetometer)
- **Tables**: Default paths `./solar_wind_table`, `./magnetometer_table` (auto-created)
- **Integration tests**: `tests/integration_tests.rs`
- **Error handling**: Custom `SwpcDeltaError` type with proper error propagation

## CLI Options
- `--solar-wind-path`: Directory for solar wind Delta Lake table (default: ./solar_wind_table)
- `--magnetometer-path`: Directory for magnetometer Delta Lake table (default: ./magnetometer_table)
- `--skip-optimization`: Skip table optimization and vacuum for faster ingestion

## Code Style
- Use snake_case for functions/variables, PascalCase for structs/enums
- Imports: std → external crates → local modules
- Error handling: Use `thiserror` for custom errors, `Result<T, E>` returns
- Async: Tokio runtime, async/await patterns
- Documentation: Document all public APIs with `///`
- No unsafe code, prefer streaming for large data
