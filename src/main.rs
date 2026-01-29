#![deny(
    warnings,
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo
)]

use anyhow::{Context, Result};
use chrono::TimeZone;
use chrono::{DateTime, Duration, FixedOffset, Utc};
use clap::Parser;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use regex::Regex;
use rusqlite::Connection;
use std::collections::HashMap;
use std::fs;
use std::io::{BufReader, Cursor};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};
use walkdir::WalkDir;

static TS_RE: LazyLock<Regex> = LazyLock::new(|| {
    // Matches Gadgetbridge's filename timestamp style:
    // 2026-01-29T08_25_59+01_00  (underscores instead of colons)
    Regex::new(r"(\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}[+-]\d{2}_\d{2})").unwrap()
});

static DEBUG: AtomicBool = AtomicBool::new(false);

macro_rules! dlog {
    ($($arg:tt)*) => {
        if DEBUG.load(Ordering::Relaxed) {
            eprintln!($($arg)*);
        }
    };
}

#[derive(Debug, Clone)]
struct Workout {
    start: DateTime<Utc>,
    duration: Option<Duration>,
    source: String,
}

#[derive(Parser, Debug)]
#[command(
    name = "roudenn",
    about = "Extract workouts from a Gadgetbridge export"
)]
#[allow(clippy::struct_excessive_bools)]
struct Args {
    /// Path to the Gadgetbridge export directory (contains gadgetbridge.json, files/, database/, etc.)
    export_dir: PathBuf,

    /// How many recent workouts to print
    #[arg(long, default_value_t = 5)]
    count: usize,

    /// Print start time + source along with duration
    #[arg(long)]
    verbose: bool,

    /// Disable reading from the `SQLite` database (`database/Gadgetbridge`)
    #[arg(long)]
    no_db: bool,

    /// Disable reading from GPX files (`files/*.gpx`)
    #[arg(long)]
    no_gpx: bool,

    /// Emit debug diagnostics to stderr
    #[arg(long)]
    debug: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    DEBUG.store(args.debug, Ordering::Relaxed);
    let export_dir = args.export_dir.display();
    dlog!("debug=on export_dir={export_dir}");

    let mut workouts: Vec<Workout> = Vec::new();

    let mut gpx_total = 0usize;
    let mut gpx_known = 0usize;
    if !args.no_gpx {
        let gpx = collect_from_gpx(&args.export_dir)?;
        gpx_total = gpx.len();
        gpx_known = gpx.iter().filter(|w| w.duration.is_some()).count();
        workouts.extend(gpx);
    }

    let mut db_total = 0usize;
    let mut db_known = 0usize;
    if !args.no_db {
        let db = collect_from_db(&args.export_dir)?;
        db_total = db.len();
        db_known = db.iter().filter(|w| w.duration.is_some()).count();
        workouts.extend(db);
    }

    dlog!(
        "collected gpx_total={gpx_total} gpx_known={gpx_known} db_total={db_total} db_known={db_known}"
    );

    let merged = merge_by_start_minute(workouts);
    let merged_total = merged.len();
    let merged_unknown = merged.iter().filter(|w| w.duration.is_none()).count();
    dlog!("merged_total={merged_total} merged_unknown={merged_unknown}");

    for w in merged.iter().take(10) {
        let start_s = w.start.to_rfc3339();
        let dur_s = w
            .duration
            .map_or_else(|| "unknown".to_string(), format_duration);
        let source = &w.source;
        dlog!("merged_item start={start_s} dur={dur_s} source={source}");
    }

    if merged.is_empty() {
        anyhow::bail!(
            "No workouts found. Check that you passed the Gadgetbridge export root directory."
        );
    }

    for (i, w) in merged.into_iter().take(args.count).enumerate() {
        let Workout {
            start,
            duration,
            source,
        } = w;

        let dur_str = duration.map_or_else(|| "unknown".to_string(), format_duration);

        if args.verbose {
            let start_s = start.to_rfc3339();
            println!("{}\t{start_s}\t{dur_str}\t{source}", i + 1);
        } else {
            println!("{dur_str}");
        }
    }

    Ok(())
}

// ---------------------------- GPX ---------------------------------

fn collect_from_gpx(export_dir: &Path) -> Result<Vec<Workout>> {
    let files_dir = export_dir.join("files");
    if !files_dir.exists() {
        return Ok(Vec::new());
    }

    let mut out = Vec::new();

    let mut seen = 0usize;
    let mut start_fail = 0usize;
    let mut empty = 0usize;
    let mut no_duration = 0usize;
    let mut with_duration = 0usize;

    let mut sample_empty = 0usize;
    let mut sample_nodur = 0usize;

    for entry in WalkDir::new(&files_dir)
        .into_iter()
        .filter_map(std::result::Result::ok)
    {
        if !entry.file_type().is_file() {
            continue;
        }

        let path = entry.path();
        if path.extension().and_then(|s| s.to_str()) != Some("gpx") {
            continue;
        }

        seen += 1;

        let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
        if size == 0 {
            empty += 1;
            if sample_empty < 5 {
                let p = path.display();
                dlog!("gpx_empty path={p}");
                sample_empty += 1;
            }
        }

        let Some(file_name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };

        let Some(start) = parse_start_from_filename(file_name) else {
            start_fail += 1;
            continue;
        };

        let display = path.display();
        let duration =
            duration_from_gpx(path).with_context(|| format!("Parsing GPX: {display}"))?;

        if duration.is_some() {
            with_duration += 1;
        } else {
            no_duration += 1;
            if sample_nodur < 5 {
                let p = path.display();
                dlog!("gpx_no_duration path={p} size={size}");
                sample_nodur += 1;
            }
        }

        out.push(Workout {
            start,
            duration,
            source: format!("gpx:{file_name}"),
        });
    }

    dlog!(
        "gpx_summary seen={seen} start_fail={start_fail} empty={empty} with_duration={with_duration} no_duration={no_duration}"
    );

    out.sort_by(|a, b| b.start.cmp(&a.start));
    Ok(out)
}

fn parse_start_from_filename(file_name: &str) -> Option<DateTime<Utc>> {
    let caps = TS_RE.captures(file_name)?;
    let raw = caps.get(1)?.as_str();

    // Convert underscores to colons to get RFC3339:
    // 2026-01-29T08_25_59+01_00 -> 2026-01-29T08:25:59+01:00
    let rfc3339 = raw.replace('_', ":");
    let dt_fixed: DateTime<FixedOffset> = DateTime::parse_from_rfc3339(&rfc3339).ok()?;
    Some(dt_fixed.with_timezone(&Utc))
}

fn duration_from_gpx(path: &Path) -> Result<Option<Duration>> {
    let bytes = fs::read(path)?;
    if bytes.is_empty() {
        return Ok(None);
    }

    let cursor = Cursor::new(bytes);
    let reader = BufReader::new(cursor);
    let mut xml = Reader::from_reader(reader);
    xml.config_mut().trim_text(true);

    let mut buf = Vec::new();
    let mut expecting_time_text = false;

    let mut min_t: Option<DateTime<Utc>> = None;
    let mut max_t: Option<DateTime<Utc>> = None;

    let mut time_count = 0usize;

    loop {
        match xml.read_event_into(&mut buf) {
            Ok(Event::Eof) => break,
            Ok(Event::Start(e)) => {
                if e.name().as_ref() == b"time" {
                    expecting_time_text = true;
                }
            }
            Ok(Event::End(e)) => {
                if e.name().as_ref() == b"time" {
                    expecting_time_text = false;
                }
            }
            Ok(Event::Text(e)) => {
                if expecting_time_text
                    && let Ok(s) = e.decode()
                    && let Ok(dt_fixed) = DateTime::parse_from_rfc3339(s.as_ref())
                {
                    time_count += 1;

                    let dt = dt_fixed.with_timezone(&Utc);
                    min_t = Some(min_t.map_or(dt, |cur| cur.min(dt)));
                    max_t = Some(max_t.map_or(dt, |cur| cur.max(dt)));
                }
            }
            Err(e) => {
                let p = path.display();
                dlog!("gpx_xml_error path={p} err={e}");
                return Ok(None);
            }
            _ => {}
        }

        buf.clear();
    }

    if time_count == 0 {
        let p = path.display();
        dlog!("gpx_no_time_elements path={p}");
    }

    match (min_t, max_t) {
        (Some(a), Some(b)) if b > a => Ok(Some(b - a)),
        _ => Ok(None),
    }
}

// ---------------------------- SQLite DB ---------------------------------

fn collect_from_db(export_dir: &Path) -> Result<Vec<Workout>> {
    let db_path = export_dir.join("database").join("Gadgetbridge");
    if !db_path.exists() {
        return Ok(Vec::new());
    }

    let display = db_path.display();
    let conn =
        Connection::open(&db_path).with_context(|| format!("Opening SQLite DB: {display}"))?;

    // For Amazfit/Huami exports, this is the correct *summary* table (one row per workout).
    if table_exists(&conn, "BASE_ACTIVITY_SUMMARY")? {
        let out = collect_from_base_activity_summary(&conn)?;
        let total = out.len();
        let known = out.iter().filter(|w| w.duration.is_some()).count();
        dlog!("db_base_activity_summary total={total} known={known}");
        return Ok(out);
    }

    anyhow::bail!(
        "SQLite DB does not contain BASE_ACTIVITY_SUMMARY (cannot derive workout durations)."
    );
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    let mut stmt =
        conn.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1 LIMIT 1")?;
    let mut rows = stmt.query([table])?;
    Ok(rows.next()?.is_some())
}

fn collect_from_base_activity_summary(conn: &Connection) -> Result<Vec<Workout>> {
    // START_TIME / END_TIME are epoch milliseconds (as shown in your sqlite output).
    let sql = r#"
        SELECT
            START_TIME,
            END_TIME,
            ACTIVITY_KIND,
            GPX_TRACK
        FROM BASE_ACTIVITY_SUMMARY
        ORDER BY START_TIME DESC
        LIMIT 2000
    "#;

    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query([])?;

    let mut out = Vec::new();

    while let Some(row) = rows.next()? {
        let start_ms: i64 = row.get(0)?;
        let end_ms: i64 = row.get(1)?;
        let activity_kind: i64 = row.get(2)?;
        let gpx_track: Option<String> = row.get(3)?;

        let Some(start) = Utc.timestamp_millis_opt(start_ms).single() else {
            dlog!("db_bad_start_ms start_ms={start_ms}");
            continue;
        };
        let Some(end) = Utc.timestamp_millis_opt(end_ms).single() else {
            dlog!("db_bad_end_ms end_ms={end_ms}");
            continue;
        };

        let duration = (end > start).then_some(end - start);

        let gpx_hint = gpx_track.as_deref().unwrap_or("-");
        let source = format!("db:BASE_ACTIVITY_SUMMARY kind={activity_kind} gpx={gpx_hint}");

        out.push(Workout {
            start,
            duration,
            source,
        });
    }

    Ok(out)
}

// ---------------------------- Merge & formatting ---------------------------------

fn merge_by_start_minute(workouts: Vec<Workout>) -> Vec<Workout> {
    // Key by "start minute" to dedupe GPX vs DB entries for the same workout.
    let mut by_key: HashMap<i64, Workout> = HashMap::new();

    let mut sorted = workouts;
    sorted.sort_by(|a, b| b.start.cmp(&a.start));

    for w in sorted {
        let key = w.start.timestamp() / 60;
        match by_key.get(&key) {
            None => {
                by_key.insert(key, w);
            }
            Some(existing) => {
                if choose_better(existing, &w) {
                    by_key.insert(key, w);
                }
            }
        }
    }

    let mut out = by_key.into_values().collect::<Vec<_>>();
    out.sort_by(|a, b| b.start.cmp(&a.start));
    out
}

fn choose_better(a: &Workout, b: &Workout) -> bool {
    match (a.duration.is_some(), b.duration.is_some()) {
        (false, true) => true,
        (true, false) => false,
        _ => {
            // Prefer DB over GPX when both have durations (DB is authoritative here).
            let a_db = a.source.starts_with("db:");
            let b_db = b.source.starts_with("db:");
            b_db && !a_db
        }
    }
}

fn format_duration(d: Duration) -> String {
    let secs = d.num_seconds().unsigned_abs();
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{h:02}:{m:02}:{s:02}")
}
