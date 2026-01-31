use clap::{Parser, Subcommand};
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(
    name = "roudenn",
    about = "Extract workouts from a Gadgetbridge export"
)]
#[allow(clippy::struct_excessive_bools)]
pub struct Cli {
    /// Path to the Gadgetbridge export directory (contains gadgetbridge.json, files/, database/, etc.)
    ///
    /// If you use the `ingest` subcommand, the export dir is provided there instead.
    pub export_dir: Option<PathBuf>,

    /// How many recent workouts to print
    #[arg(long, default_value_t = 5)]
    pub count: usize,

    /// Print start time + source along with duration
    #[arg(long)]
    pub verbose: bool,

    /// Disable reading from the `SQLite` database (`database/Gadgetbridge`)
    #[arg(long)]
    pub no_db: bool,

    /// Disable reading from GPX files (`files/*.gpx`)
    #[arg(long)]
    pub no_gpx: bool,

    /// Emit debug diagnostics to stderr
    #[arg(long)]
    pub debug: bool,

    #[command(subcommand)]
    pub cmd: Option<Cmd>,
}

#[derive(Subcommand, Debug)]
pub enum Cmd {
    /// Import workouts into `PostgreSQL` for Grafana
    Ingest {
        /// Path to the Gadgetbridge export directory
        export_dir: PathBuf,

        /// `PostgreSQL` connection URL (e.g. `<postgres://user:pass@127.0.0.1:5432/fitness>`)
        #[arg(long)]
        pg_url: String,

        /// Also parse GPX tracks referenced by `BASE_ACTIVITY_SUMMARY` and import points
        #[arg(long)]
        with_points: bool,

        /// Also read rawDetails/*.bin and store as bytea (can be large)
        #[arg(long)]
        store_raw_details: bool,

        /// Emit debug diagnostics to stderr
        #[arg(long)]
        debug: bool,
    },
}
