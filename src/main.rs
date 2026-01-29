#![deny(
    warnings,
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo
)]
#![allow(clippy::multiple_crate_versions)]

use anyhow::{Context, Result};
use chrono::TimeZone;
use chrono::{DateTime, Duration, FixedOffset, Utc};
use clap::{Parser, Subcommand};
use postgres::{Client, NoTls};
use quick_xml::events::{BytesEnd, BytesStart, Event};
use quick_xml::reader::Reader;
use regex::Regex;
use rusqlite::Connection;
use serde_json::Value as JsonValue;
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

#[derive(Debug, Clone)]
struct WorkoutSummary {
    name: Option<String>,
    start: DateTime<Utc>,
    end: DateTime<Utc>,
    activity_kind: i32,

    base_longitude_e7: Option<i64>,
    base_latitude_e7: Option<i64>,
    base_altitude: Option<i64>,

    gpx_track_android: Option<String>,
    raw_details_android: Option<String>,

    device_id: i32,
    user_id: i32,

    summary_data_raw: Option<String>,
    summary_data_json: Option<JsonValue>,
    raw_summary_data: Option<Vec<u8>>,

    raw_details: Option<Vec<u8>>,
}

#[derive(Parser, Debug)]
#[command(
    name = "roudenn",
    about = "Extract workouts from a Gadgetbridge export"
)]
#[allow(clippy::struct_excessive_bools)]
struct Cli {
    /// Path to the Gadgetbridge export directory (contains gadgetbridge.json, files/, database/, etc.)
    ///
    /// If you use the `ingest` subcommand, the export dir is provided there instead.
    export_dir: Option<PathBuf>,

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

    #[command(subcommand)]
    cmd: Option<Cmd>,
}

#[derive(Subcommand, Debug)]
enum Cmd {
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

fn main() -> Result<()> {
    let cli = Cli::parse();

    match cli.cmd {
        Some(Cmd::Ingest {
            export_dir,
            pg_url,
            with_points,
            store_raw_details,
            debug,
        }) => {
            DEBUG.store(debug, Ordering::Relaxed);
            let d = export_dir.display();
            dlog!(
                "mode=ingest export_dir={d} with_points={with_points} store_raw_details={store_raw_details}"
            );

            ingest(&export_dir, &pg_url, with_points, store_raw_details)?;
            Ok(())
        }
        None => {
            DEBUG.store(cli.debug, Ordering::Relaxed);

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
                let gpx = collect_from_gpx(&export_dir)?;
                gpx_total = gpx.len();
                gpx_known = gpx.iter().filter(|w| w.duration.is_some()).count();
                workouts.extend(gpx);
            }

            let mut db_total = 0usize;
            let mut db_known = 0usize;
            if !cli.no_db {
                let db = collect_from_db(&export_dir)?;
                db_total = db.len();
                db_known = db.iter().filter(|w| w.duration.is_some()).count();
                workouts.extend(db);
            }

            dlog!(
                "collected gpx_total={gpx_total} gpx_known={gpx_known} db_total={db_total} db_known={db_known}"
            );

            let merged = merge_by_start_minute(workouts);
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

                let dur_str = duration.map_or_else(|| "unknown".to_string(), format_duration);

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

// ---------------------------- Ingest (PostgreSQL) ---------------------------------

fn ingest(
    export_dir: &Path,
    pg_url: &str,
    with_points: bool,
    store_raw_details: bool,
) -> Result<()> {
    let mut pg = Client::connect(pg_url, NoTls).context("Connecting to PostgreSQL")?;
    ensure_pg_schema(&mut pg)?;

    let summaries = read_base_activity_summary(export_dir, store_raw_details)?;
    let total = summaries.len();
    dlog!("found summaries={total}");

    let mut inserted_or_updated = 0usize;
    let mut points_imported = 0usize;
    let mut workouts_with_points = 0usize;

    for s in summaries {
        let workout_id = upsert_workout(&mut pg, &s)?;
        inserted_or_updated += 1;

        if with_points {
            let Some(android_path) = s.gpx_track_android.as_deref() else {
                continue;
            };
            let Some(gpx_path) = map_android_gpx_to_export(export_dir, android_path) else {
                dlog!("gpx_map_failed android_path={android_path}");
                continue;
            };
            if !gpx_path.exists() {
                let p = gpx_path.display();
                dlog!("gpx_missing path={p}");
                continue;
            }

            let pts = parse_gpx_points(&gpx_path)
                .with_context(|| format!("Parsing GPX points: {}", gpx_path.display()))?;
            if pts.is_empty() {
                continue;
            }

            import_points_for_workout(&mut pg, workout_id, &pts)?;
            workouts_with_points += 1;
            points_imported += pts.len();
        }
    }

    eprintln!(
        "ingest done: workouts_upserted={inserted_or_updated} workouts_with_points={workouts_with_points} points_imported={points_imported}"
    );

    Ok(())
}

fn ensure_pg_schema(pg: &mut Client) -> Result<()> {
    pg.batch_execute(
        r"
        CREATE TABLE IF NOT EXISTS workouts (
          id                 bigserial PRIMARY KEY,
          device_id          int NOT NULL,
          user_id            int NOT NULL,
          activity_kind      int NOT NULL,

          start_time         timestamptz NOT NULL,
          end_time           timestamptz NOT NULL,
          duration_s         int NOT NULL,

          name               text,

          base_longitude_e7  bigint,
          base_latitude_e7   bigint,
          base_altitude      bigint,

          base_lon           double precision,
          base_lat           double precision,

          gpx_track_android  text,
          raw_details_android text,

          summary_data_raw   text,
          summary_data_json  jsonb,

          raw_summary_data   bytea,
          raw_details        bytea,

          created_at         timestamptz NOT NULL DEFAULT now(),
          updated_at         timestamptz NOT NULL DEFAULT now(),

          UNIQUE (device_id, start_time)
        );

        CREATE INDEX IF NOT EXISTS workouts_start_time_idx ON workouts (start_time DESC);
        CREATE INDEX IF NOT EXISTS workouts_kind_idx ON workouts (activity_kind);

        CREATE TABLE IF NOT EXISTS workout_points (
          workout_id  bigint NOT NULL REFERENCES workouts(id) ON DELETE CASCADE,
          idx         int NOT NULL,
          t           timestamptz NOT NULL,
          lat         double precision NOT NULL,
          lon         double precision NOT NULL,
          ele         double precision,
          PRIMARY KEY (workout_id, idx)
        );

        CREATE INDEX IF NOT EXISTS workout_points_t_idx ON workout_points (t);
        ",
    )
    .context("Ensuring PostgreSQL schema")?;

    Ok(())
}

fn upsert_workout(pg: &mut Client, s: &WorkoutSummary) -> Result<i64> {
    let duration_s_i32 = duration_seconds_i32(s.end - s.start);
    let (base_lon, base_lat) = e7_to_degrees(s.base_longitude_e7, s.base_latitude_e7);

    let summary_json = s.summary_data_json.as_ref();
    let raw_summary_data = s.raw_summary_data.as_deref();
    let raw_details = s.raw_details.as_deref();

    let row = pg
        .query_one(
            r"
            INSERT INTO workouts (
              device_id, user_id, activity_kind,
              start_time, end_time, duration_s,
              name,
              base_longitude_e7, base_latitude_e7, base_altitude,
              base_lon, base_lat,
              gpx_track_android, raw_details_android,
              summary_data_raw, summary_data_json,
              raw_summary_data, raw_details,
              updated_at
            )
            VALUES (
              $1, $2, $3,
              $4, $5, $6,
              $7,
              $8, $9, $10,
              $11, $12,
              $13, $14,
              $15, $16,
              $17, $18,
              now()
            )
            ON CONFLICT (device_id, start_time) DO UPDATE SET
              user_id = EXCLUDED.user_id,
              activity_kind = EXCLUDED.activity_kind,
              end_time = EXCLUDED.end_time,
              duration_s = EXCLUDED.duration_s,
              name = EXCLUDED.name,
              base_longitude_e7 = EXCLUDED.base_longitude_e7,
              base_latitude_e7 = EXCLUDED.base_latitude_e7,
              base_altitude = EXCLUDED.base_altitude,
              base_lon = EXCLUDED.base_lon,
              base_lat = EXCLUDED.base_lat,
              gpx_track_android = EXCLUDED.gpx_track_android,
              raw_details_android = EXCLUDED.raw_details_android,
              summary_data_raw = EXCLUDED.summary_data_raw,
              summary_data_json = EXCLUDED.summary_data_json,
              raw_summary_data = EXCLUDED.raw_summary_data,
              raw_details = EXCLUDED.raw_details,
              updated_at = now()
            RETURNING id
            ",
            &[
                &s.device_id,
                &s.user_id,
                &s.activity_kind,
                &s.start,
                &s.end,
                &duration_s_i32,
                &s.name,
                &s.base_longitude_e7,
                &s.base_latitude_e7,
                &s.base_altitude,
                &base_lon,
                &base_lat,
                &s.gpx_track_android,
                &s.raw_details_android,
                &s.summary_data_raw,
                &summary_json,
                &raw_summary_data,
                &raw_details,
            ],
        )
        .context("Upserting workout")?;

    let id: i64 = row.get(0);
    Ok(id)
}

fn import_points_for_workout(pg: &mut Client, workout_id: i64, points: &[GpxPoint]) -> Result<()> {
    let mut tx = pg
        .transaction()
        .context("Starting transaction for points")?;

    tx.execute(
        "DELETE FROM workout_points WHERE workout_id=$1",
        &[&workout_id],
    )
    .context("Deleting existing points")?;

    let stmt = tx
        .prepare(
            "INSERT INTO workout_points (workout_id, idx, t, lat, lon, ele) VALUES ($1, $2, $3, $4, $5, $6)",
        )
        .context("Preparing point insert")?;

    for p in points {
        tx.execute(&stmt, &[&workout_id, &p.idx, &p.t, &p.lat, &p.lon, &p.ele])
            .context("Inserting point")?;
    }

    tx.commit().context("Committing points transaction")?;
    Ok(())
}

fn duration_seconds_i32(d: Duration) -> i32 {
    let secs = d.num_seconds().abs();
    i32::try_from(secs).unwrap_or(i32::MAX)
}

fn e7_to_degrees(lon_e7: Option<i64>, lat_e7: Option<i64>) -> (Option<f64>, Option<f64>) {
    // Avoid clippy::cast_precision_loss by converting via i32 -> f64 (exact for your E7 ranges).
    let denom = 10_000_000.0_f64;

    let lon = lon_e7
        .and_then(|v| i32::try_from(v).ok())
        .map(|v| f64::from(v) / denom);

    let lat = lat_e7
        .and_then(|v| i32::try_from(v).ok())
        .map(|v| f64::from(v) / denom);

    (lon, lat)
}

// ---------------------------- GPX parsing (print mode) ---------------------------------

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

#[derive(Debug, Clone)]
struct GpxPoint {
    idx: i32,
    t: DateTime<Utc>,
    lat: f64,
    lon: f64,
    ele: Option<f64>,
}

fn parse_gpx_points(path: &Path) -> Result<Vec<GpxPoint>> {
    let bytes = fs::read(path)?;
    if bytes.is_empty() {
        return Ok(Vec::new());
    }

    let cursor = Cursor::new(bytes);
    let reader = BufReader::new(cursor);
    let mut xml = Reader::from_reader(reader);
    xml.config_mut().trim_text(true);

    let mut buf = Vec::new();

    let mut st = GpxState::default();
    let mut out: Vec<GpxPoint> = Vec::new();

    loop {
        match xml.read_event_into(&mut buf) {
            Ok(Event::Eof) => break,
            Ok(Event::Start(e)) => handle_gpx_start(&mut st, &e),
            Ok(Event::End(e)) => handle_gpx_end(&mut st, &e, &mut out),
            Ok(Event::Text(e)) => {
                handle_gpx_text(&mut st, &e);
            }
            Err(e) => anyhow::bail!("GPX XML parse error: {e}"),
            _ => {}
        }
        buf.clear();
    }

    Ok(out)
}

#[derive(Default)]
struct GpxState {
    in_trkpt: bool,
    in_time: bool,
    in_ele: bool,

    cur_lat: Option<f64>,
    cur_lon: Option<f64>,
    cur_time: Option<DateTime<Utc>>,
    cur_ele: Option<f64>,

    idx: i32,
}

fn handle_gpx_start(st: &mut GpxState, e: &BytesStart<'_>) {
    match e.name().as_ref() {
        b"trkpt" => {
            st.in_trkpt = true;
            st.in_time = false;
            st.in_ele = false;

            st.cur_lat = None;
            st.cur_lon = None;
            st.cur_time = None;
            st.cur_ele = None;

            let (lat, lon) = parse_trkpt_lat_lon(e);
            st.cur_lat = lat;
            st.cur_lon = lon;
        }
        b"time" if st.in_trkpt => {
            st.in_time = true;
        }
        b"ele" if st.in_trkpt => {
            st.in_ele = true;
        }
        _ => {}
    }
}

fn handle_gpx_end(st: &mut GpxState, e: &BytesEnd<'_>, out: &mut Vec<GpxPoint>) {
    match e.name().as_ref() {
        b"time" => st.in_time = false,
        b"ele" => st.in_ele = false,
        b"trkpt" => {
            st.in_trkpt = false;

            let Some(lat) = st.cur_lat else {
                return;
            };
            let Some(lon) = st.cur_lon else {
                return;
            };
            let Some(t) = st.cur_time else {
                return;
            };

            out.push(GpxPoint {
                idx: st.idx,
                t,
                lat,
                lon,
                ele: st.cur_ele,
            });
            st.idx = st.idx.saturating_add(1);
        }
        _ => {}
    }
}

fn handle_gpx_text(st: &mut GpxState, e: &quick_xml::events::BytesText<'_>) {
    if st.in_time
        && let Ok(s) = e.decode()
        && let Ok(dt_fixed) = DateTime::parse_from_rfc3339(s.as_ref())
    {
        st.cur_time = Some(dt_fixed.with_timezone(&Utc));
    } else if st.in_ele
        && let Ok(s) = e.decode()
        && let Ok(v) = s.parse::<f64>()
    {
        st.cur_ele = Some(v);
    }
}

fn parse_trkpt_lat_lon(e: &BytesStart<'_>) -> (Option<f64>, Option<f64>) {
    let mut lat: Option<f64> = None;
    let mut lon: Option<f64> = None;

    for a in e.attributes().with_checks(false).flatten() {
        let key = a.key.as_ref();
        if key == b"lat"
            && let Ok(v) = a.unescape_value()
        {
            lat = v.parse::<f64>().ok();
        } else if key == b"lon"
            && let Ok(v) = a.unescape_value()
        {
            lon = v.parse::<f64>().ok();
        }
    }

    (lat, lon)
}

fn map_android_gpx_to_export(export_dir: &Path, android_path: &str) -> Option<PathBuf> {
    let file_name = Path::new(android_path).file_name()?.to_str()?;
    Some(export_dir.join("files").join(file_name))
}

fn map_android_raw_details_to_export(export_dir: &Path, android_path: &str) -> Option<PathBuf> {
    let file_name = Path::new(android_path).file_name()?.to_str()?;
    Some(export_dir.join("files").join("rawDetails").join(file_name))
}

// ---------------------------- SQLite (print mode + ingest) ---------------------------------

fn collect_from_db(export_dir: &Path) -> Result<Vec<Workout>> {
    let summaries = read_base_activity_summary(export_dir, false)?;
    let mut out = Vec::with_capacity(summaries.len());

    for s in summaries {
        let duration = (s.end > s.start).then_some(s.end - s.start);
        out.push(Workout {
            start: s.start,
            duration,
            source: format!("db:BASE_ACTIVITY_SUMMARY kind={}", s.activity_kind),
        });
    }

    Ok(out)
}

fn read_base_activity_summary(
    export_dir: &Path,
    store_raw_details: bool,
) -> Result<Vec<WorkoutSummary>> {
    let db_path = export_dir.join("database").join("Gadgetbridge");
    if !db_path.exists() {
        return Ok(Vec::new());
    }

    let display = db_path.display();
    let conn =
        Connection::open(&db_path).with_context(|| format!("Opening SQLite DB: {display}"))?;

    if !table_exists(&conn, "BASE_ACTIVITY_SUMMARY")? {
        anyhow::bail!("SQLite DB does not contain BASE_ACTIVITY_SUMMARY.");
    }

    let sql = r"
        SELECT
            _id,
            NAME,
            START_TIME,
            END_TIME,
            ACTIVITY_KIND,
            BASE_LONGITUDE,
            BASE_LATITUDE,
            BASE_ALTITUDE,
            GPX_TRACK,
            RAW_DETAILS_PATH,
            DEVICE_ID,
            USER_ID,
            SUMMARY_DATA,
            RAW_SUMMARY_DATA
        FROM BASE_ACTIVITY_SUMMARY
        ORDER BY START_TIME DESC
    ";

    let mut stmt = conn.prepare(sql)?;
    let mut rows = stmt.query([])?;

    let mut out: Vec<WorkoutSummary> = Vec::new();

    while let Some(row) = rows.next()? {
        let sqlite_id: i64 = row.get(0)?;
        let name: Option<String> = row.get(1)?;

        let start_ms: i64 = row.get(2)?;
        let end_ms: i64 = row.get(3)?;

        let activity_kind_i64: i64 = row.get(4)?;
        let activity_kind = i32::try_from(activity_kind_i64).unwrap_or(i32::MAX);

        let base_longitude_e7: Option<i64> = row.get(5)?;
        let base_latitude_e7: Option<i64> = row.get(6)?;
        let base_altitude: Option<i64> = row.get(7)?;

        let gpx_track_android: Option<String> = row.get(8)?;
        let raw_details_android: Option<String> = row.get(9)?;

        let device_id_i64: i64 = row.get(10)?;
        let user_id_i64: i64 = row.get(11)?;
        let device_id = i32::try_from(device_id_i64).unwrap_or(i32::MAX);
        let user_id = i32::try_from(user_id_i64).unwrap_or(i32::MAX);

        let summary_data_raw: Option<String> = row.get(12)?;
        let summary_data_json = summary_data_raw
            .as_deref()
            .and_then(|s| serde_json::from_str::<JsonValue>(s).ok());

        let raw_summary_data: Option<Vec<u8>> = row.get(13)?;

        let Some(start) = Utc.timestamp_millis_opt(start_ms).single() else {
            dlog!("db_bad_start_ms start_ms={start_ms} sqlite_id={sqlite_id}");
            continue;
        };
        let Some(end) = Utc.timestamp_millis_opt(end_ms).single() else {
            dlog!("db_bad_end_ms end_ms={end_ms} sqlite_id={sqlite_id}");
            continue;
        };

        let raw_details = if store_raw_details {
            raw_details_android
                .as_deref()
                .and_then(|p| map_android_raw_details_to_export(export_dir, p))
                .and_then(|p| fs::read(p).ok())
        } else {
            None
        };

        out.push(WorkoutSummary {
            name,
            start,
            end,
            activity_kind,

            base_longitude_e7,
            base_latitude_e7,
            base_altitude,

            gpx_track_android,
            raw_details_android,

            device_id,
            user_id,

            summary_data_raw,
            summary_data_json,
            raw_summary_data,

            raw_details,
        });
    }

    Ok(out)
}

fn table_exists(conn: &Connection, table: &str) -> Result<bool> {
    let mut stmt =
        conn.prepare("SELECT 1 FROM sqlite_master WHERE type='table' AND name=?1 LIMIT 1")?;
    let mut rows = stmt.query([table])?;
    Ok(rows.next()?.is_some())
}

// ---------------------------- Merge & formatting (print mode) ---------------------------------

fn merge_by_start_minute(workouts: Vec<Workout>) -> Vec<Workout> {
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
