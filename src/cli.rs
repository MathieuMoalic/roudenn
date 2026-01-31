use clap::{ArgAction, Parser, Subcommand};
use std::path::PathBuf;

const DEFAULT_EXPORT_ZIP: &str = "/home/mat/docs/personal/GadgetBridge/Gadgetbridge.zip";

#[derive(Parser, Debug)]
#[command(
    name = "roudenn",
    about = "Extract workouts from a Gadgetbridge export (ZIP or dir)"
)]
pub struct Cli {
    /// Path to the Gadgetbridge export ZIP (or an already-extracted export directory).
    ///
    /// Default: /home/mat/docs/personal/GadgetBridge/Gadgetbridge.zip
    #[arg(value_name = "EXPORT", default_value = DEFAULT_EXPORT_ZIP)]
    pub export: PathBuf,

    /// How many recent workouts to print
    #[arg(long, default_value_t = 5)]
    pub count: usize,

    /// Print start time + source along with duration
    #[arg(long)]
    pub details: bool,

    /// Disable reading from the SQLite database (`database/Gadgetbridge`)
    #[arg(long)]
    pub no_db: bool,

    /// Disable reading from GPX files (`files/*.gpx`)
    #[arg(long)]
    pub no_gpx: bool,

    /// Increase log verbosity (-v, -vv). Defaults to INFO.
    #[arg(short = 'v', long, action = ArgAction::Count, global = true)]
    pub verbose: u8,

    /// Decrease log verbosity (-q, -qq). Defaults to INFO.
    #[arg(short = 'q', long, action = ArgAction::Count, global = true)]
    pub quiet: u8,

    #[command(subcommand)]
    pub cmd: Option<Cmd>,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Import workouts into PostgreSQL for Grafana
    Ingest {
        /// Path to the Gadgetbridge export ZIP (or already-extracted export directory).
        #[arg(value_name = "EXPORT", default_value = DEFAULT_EXPORT_ZIP)]
        export: PathBuf,

        /// PostgreSQL connection URL (e.g. `postgres://user:pass@127.0.0.1:5432/fitness`)
        #[arg(long)]
        pg_url: String,

        /// Also parse GPX tracks referenced by BASE_ACTIVITY_SUMMARY and import points
        #[arg(long)]
        with_points: bool,

        /// Also read rawDetails/*.bin and store as bytea (can be large)
        #[arg(long)]
        store_raw_details: bool,
    },
}
