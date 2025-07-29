use clap::Parser;

#[derive(Parser, Debug)]
#[command(name = "swpc_delta")]
#[command(about = "SWPC Solar Wind and Magnetometer data ingestion to Delta Lake")]
pub struct Args {
    /// Solar wind Delta Lake table directory path
    #[clap(
        long,
        default_value = "./solar_wind",
        help = "Path to solar wind Delta Lake table directory"
    )]
    pub solar_wind_path: String,

    /// Magnetometer Delta Lake table directory path
    #[clap(
        long,
        default_value = "./magnetometer",
        help = "Path to magnetometer Delta Lake table directory"
    )]
    pub magnetometer_path: String,

    /// Skip optimization and vacuum for faster ingestion
    #[clap(long, help = "Skip table optimization and vacuum operations")]
    pub skip_optimization: bool,
}
