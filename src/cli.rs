use clap::{ArgAction, Parser};
use std::path::PathBuf;

const DEFAULT_EXPORT_ZIP: &str = "/home/mat/docs/personal/GadgetBridge/Gadgetbridge.zip";
const DEFAULT_PG_URL: &str = "postgres://127.0.0.1:5432/fitness";

#[derive(Parser, Debug)]
#[command(
    name = "roudenn",
    about = "Import workouts from a Gadgetbridge export (ZIP or dir) into PostgreSQL"
)]
pub struct Cli {
    /// Path to the Gadgetbridge export ZIP (or an already-extracted export directory).
    ///
    /// Default: /home/mat/docs/personal/GadgetBridge/Gadgetbridge.zip
    #[arg(value_name = "EXPORT", default_value = DEFAULT_EXPORT_ZIP)]
    pub export: PathBuf,

    /// PostgreSQL connection URL
    #[arg(long, default_value = DEFAULT_PG_URL)]
    pub pg_url: String,

    /// Increase log verbosity (-v, -vv). Defaults to INFO.
    #[arg(short = 'v', long, action = ArgAction::Count, global = true)]
    pub verbose: u8,

    /// Decrease log verbosity (-q, -qq). Defaults to INFO.
    #[arg(short = 'q', long, action = ArgAction::Count, global = true)]
    pub quiet: u8,
}
