use clap::Parser;
use swpc_delta::{cli::Args, error::SwpcDeltaError, pipeline::run_pipeline};

#[tokio::main]
async fn main() -> Result<(), SwpcDeltaError> {
    let args = Args::parse();
    run_pipeline(args).await
}
