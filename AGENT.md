# SWPC Solar Wind Delta Lake - Agent Guidelines

## Commands
- **Build**: `cargo build` / `cargo build --release`
- **Test**: `cargo test` / `cargo test --lib` (for single test: `cargo test test_name`)
- **Lint**: `cargo clippy`
- **Format**: `cargo fmt`
- **Run**: `./target/release/swpc_delta` or `cargo run`
- **Usage**: `swpc_delta --solar-wind-path ./solar_wind --magnetometer-path ./magnetometer --skip-optimization`

## Architecture
- **Main module**: `src/main.rs` - CLI entry point with clap args for both pipelines
- **Library modules**: `src/lib.rs`, `src/delta.rs` (Delta Lake ops), `src/swpc.rs` (API client), `src/error.rs` (error types)
- **Data pipeline**: SWPC API → JSON parsing → Delta Lake tables (solar_wind + magnetometer)
- **Tables**: Default paths `./solar_wind`, `./magnetometer` (auto-created)
- **Integration tests**: `tests/integration_tests.rs`
- **Error handling**: Custom `SwpcDeltaError` type with proper error propagation

## CLI Options
- `--solar-wind-path`: Directory for solar wind Delta Lake table (default: ./solar_wind)
- `--magnetometer-path`: Directory for magnetometer Delta Lake table (default: ./magnetometer)
- `--skip-optimization`: Skip table optimization and vacuum for faster ingestion

## Code Style
- Use snake_case for functions/variables, PascalCase for structs/enums
- Imports: std → external crates → local modules
- Error handling: Use `thiserror` for custom errors, `Result<T, E>` returns
- Async: Tokio runtime, async/await patterns
- Documentation: Document all public APIs with `///`
- No unsafe code, prefer streaming for large data

## Overview
This repository demonstrates a high-performance Data Engineering Proof of Concept (POC) for ingesting real-time solar wind data from the Space Weather Prediction Center (SWPC) into Delta Lake using Rust. The project showcases efficient handling of non-standard API payloads and frequent data updates.

## Key Features
- **Real-time Data Ingestion**: Processes SWPC solar wind data with 1-minute update intervals.
- **Robust Error Handling**: Implements recovery mechanisms for failures within the last hour using historical data.
- **Delta Lake Integration**: Utilizes Delta Lake for reliable and efficient data storage and management.
- **High-Performance Processing**: Leverages Rust's performance capabilities for rapid data handling.
- **Parallel Processing**: Employs Rayon for parallel data transformation and filtering.

## Technical Stack
- **Language**: Rust
- **Data Format**: Delta Lake
- **Libraries**: 
  - `deltalake`: For Delta Lake operations
  - `tokio`: Asynchronous runtime
  - `reqwest`: HTTP client for API requests
  - `serde`: Serialization and deserialization of JSON data
  - `rayon`: Parallel data processing
  - `chrono`: Date and time handling
  - `clap`: Command-line argument parsing
  - `env_logger`: Logging support
  - `thiserror`: Error handling

## Development Best Practices

1. Run `cargo clippy` before commits
2. Use `cargo fmt` for code formatting
3. Test all changes with `cargo test`
4. Benchmark performance-critical code
5. Document all public APIs

# aer_parser LLM Agent Guidelines

## Core Principles

- **Simplicity & Maintainability:** Prioritize clear, simple code and documentation. Avoid unnecessary complexity.
- **Modular Design:** Separate concerns into distinct modules (parsing, I/O, error handling, CLI).
- **Code Deduplication:** Extract and reuse common functionality; avoid duplicate logic.
- **No Unsafe Code:** Do not use unsafe Rust code or dependencies.

## Performance & Efficiency

- **Memory Efficiency:** Use streaming/buffered operations for large files; avoid loading entire files into memory.
- **Token Efficiency:** Minimize verbose output and unnecessary back-and-forth. Use concise, context-rich responses.
- **Progress Reporting:** For long-running operations, provide progress updates.

## Documentation Standards

- **Module & Function Docs:** Document all public APIs with examples and usage patterns.
- **Inline Comments:** Explain complex logic and error handling.
- **Error Documentation:** Include recovery strategies and context in error messages.

## Refactoring Guidelines

- **Eliminate Duplication:** Refactor repeated code >20 lines.
- **Improve Readability:** Use clear variable names and add comments where necessary.
- **Simplify Logic:** Break down complex functions into smaller, reusable components.

## Token Efficiency Tips for LLM Agent

- **Context Awareness:** Use workspace and user-provided context to avoid redundant questions.
- **Concise Responses:** Prefer short, direct answers and code suggestions.
- **Batch Actions:** When possible, group related actions to minimize interaction rounds.
- **Avoid Repetition:** Reference previous context or code instead of repeating it.

## Data Pipeline
1. **Data Fetching**: Retrieves solar wind data from SWPC API.
2. **Data Transformation**: Converts non-standard payload to structured `SolarWind` objects.
3. **Filtering**: Processes only new data based on timestamp comparisons.
4. **Delta Lake Operations**: 
   - Writes data to Delta Lake format
   - Performs table optimization and vacuuming for efficient storage

---

# SWPC Delta Lake Rust Project Optimization Plan

### 1. Code Structure & Modularity
- Fix running the application without a folder so that it defaults to the default paths './solar_wind' and './magnetometer'.
- Separate parsing, I/O, error handling, and CLI logic into distinct modules.
- Refactor repeated code (batch creation, timestamp queries) into reusable functions.
- Avoid unsafe code and dependencies.

### 2. Performance
- Use iterators and streaming APIs for large data sets; avoid collecting into Vec unless necessary.
- Use Rayon parallelism only for large data; prefer sequential for small batches.
- Optimize batch sizes for Delta Lake writes to balance memory and speed.
- Always run `optimize` and `vacuum` after each incremental load to minimize file count.

### 3. Error Handling
- Use `thiserror` for rich error types; log errors with context.
- Document fallback logic (e.g., default timestamps) and make it configurable.

### 4. Testing & Validation
- Cover all public APIs and edge cases with unit and integration tests.
- Use property-based testing (e.g., `proptest`) for data transformation logic.
- Benchmark performance-critical paths with `cargo bench`.

### 5. Documentation
- Document all public items with examples.
- Add inline comments for non-obvious logic, especially error handling and parallel code.
- Document recovery strategies in error messages.

### 6. CI/CD & Tooling
- Run `cargo clippy` and `cargo fmt` before every commit.
- Automate tests with GitHub Actions or similar.
- Use Dependabot or similar for dependency updates.

### 7. Progress Reporting
- Use `env_logger` for progress and error reporting.
- Print progress for long-running operations.

### 8. Data Pipeline Improvements
- Move timestamp filtering to the earliest possible stage.
- Validate payloads before transformation; fail fast on malformed data.
- Validate Delta Lake schema compatibility before writes.

---

## Delta Lake Table Maintenance Best Practices
- Always run `optimize` and `vacuum` after each incremental load to reduce file fragmentation and improve query performance.
- Document this workflow in `.clinerules` and code comments for future maintainers.
- Monitor table file count and size periodically; adjust maintenance frequency if data volume increases.

## Other Recommendations
- Use parallelism only where beneficial.
- Keep batch sizes small and memory usage minimal.
- Maintain strong error handling and documentation.

## Rust Best Practices for SWPC Delta Lake
- Prefer explicit error handling over panics.
- Use `Result<T, E>` for all fallible operations.
- Avoid global mutable state.
- Prefer `Arc` over `Rc` for thread safety.
- Use `tokio` for async I/O; avoid blocking calls in async contexts.
- Document all public APIs and error cases.
- Refactor repeated code blocks >20 lines.
- Use clear, descriptive variable and function names.
- Batch related actions to minimize token and compute usage.
