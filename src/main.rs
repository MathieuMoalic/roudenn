#![deny(
    warnings,
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo
)]
#![allow(clippy::multiple_crate_versions)]

use anyhow::Result;
use clap::Parser;
use roudenn::{cli, ingest, utils};
extern crate roudenn;

fn main() -> Result<()> {
    let cli = cli::Cli::parse();
    utils::init_logging(cli.verbose, cli.quiet);

    let export_handle = utils::open_export(&cli.export)?;
    tracing::info!(
        export = %export_handle.dir().display(),
        pg_url = %cli.pg_url,
        "starting ingest"
    );

    ingest::ingest(export_handle.dir(), &cli.pg_url)?;
    Ok(())
}
