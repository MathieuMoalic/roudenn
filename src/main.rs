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
use roudenn::{cli, database, gpx, ingest, types::Workout, utils};

#[macro_use]
extern crate roudenn;

fn main() -> Result<()> {
    let cli = cli::Cli::parse();

    match cli.cmd {
        Some(cli::Cmd::Ingest {
            export_dir,
            pg_url,
            with_points,
            store_raw_details,
            debug,
        }) => {
            utils::set_debug(debug);
            let d = export_dir.display();
            dlog!(
                "mode=ingest export_dir={d} with_points={with_points} store_raw_details={store_raw_details}"
            );

            ingest::ingest(&export_dir, &pg_url, with_points, store_raw_details)?;
            Ok(())
        }
        None => {
            utils::set_debug(cli.debug);

            let Some(export_dir) = cli.export_dir else {
                anyhow::bail!(
                    "Missing <export_dir>. Try: roudenn <export_dir> --count 5  OR  roudenn ingest <export_dir> --pg-url ..."
                );
            };

            let export_disp = export_dir.display();
            dlog!("mode=print export_dir={export_disp}");

            let mut workouts: Vec<Workout> = Vec::new();

            let mut gpx_total = 0usize;
            let mut gpx_known = 0usize;
            if !cli.no_gpx {
                let gpx_workouts = gpx::collect_from_gpx(&export_dir)?;
                gpx_total = gpx_workouts.len();
                gpx_known = gpx_workouts.iter().filter(|w| w.duration.is_some()).count();
                workouts.extend(gpx_workouts);
            }

            let mut db_total = 0usize;
            let mut db_known = 0usize;
            if !cli.no_db {
                let db_workouts = database::collect_from_db(&export_dir)?;
                db_total = db_workouts.len();
                db_known = db_workouts.iter().filter(|w| w.duration.is_some()).count();
                workouts.extend(db_workouts);
            }

            dlog!(
                "collected gpx_total={gpx_total} gpx_known={gpx_known} db_total={db_total} db_known={db_known}"
            );

            let merged = utils::merge_by_start_minute(workouts);
            if merged.is_empty() {
                anyhow::bail!(
                    "No workouts found. Check that you passed the Gadgetbridge export root directory."
                );
            }

            for (i, w) in merged.into_iter().take(cli.count).enumerate() {
                let Workout {
                    start,
                    duration,
                    source,
                } = w;

                let dur_str =
                    duration.map_or_else(|| "unknown".to_string(), utils::format_duration);

                if cli.verbose {
                    let start_s = start.to_rfc3339();
                    println!("{}\t{start_s}\t{dur_str}\t{source}", i + 1);
                } else {
                    println!("{dur_str}");
                }
            }

            Ok(())
        }
    }
}
