#![deny(
    warnings,
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::cargo
)]

use anyhow::{Context, Result};
use chrono::TimeZone;
use chrono::{DateTime, Duration, FixedOffset, NaiveDateTime, Utc};
use clap::Parser;
use quick_xml::events::Event;
use quick_xml::reader::Reader;
use regex::Regex;
use rusqlite::types::ValueRef;
use rusqlite::{Connection, Row};
use std::collections::HashMap;
use std::fs;
use std::io::{BufReader, Cursor};
use std::path::{Path, PathBuf};
use std::sync::LazyLock;
use walkdir::WalkDir;

static TS_RE: LazyLock<Regex> = LazyLock::new(|| {
    // Matches Gadgetbridge's filename timestamp style:
    // 2026-01-29T08_25_59+01_00  (underscores instead of colons)
    Regex::new(r"(\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}[+-]\d{2}_\d{2})").unwrap()
});

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
}

fn main() -> Result<()> {
    let args = Args::parse();

    let mut workouts: Vec<Workout> = Vec::new();

    if !args.no_gpx {
        workouts.extend(collect_from_gpx(&args.export_dir)?);
    }
    if !args.no_db {
        workouts.extend(collect_from_db(&args.export_dir)?);
    }

    let merged = merge_by_start_minute(workouts);

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

        let Some(file_name) = path.file_name().and_then(|s| s.to_str()) else {
            continue;
        };

        let Some(start) = parse_start_from_filename(file_name) else {
            continue;
        };

        let display = path.display();
        let duration =
            duration_from_gpx(path).with_context(|| format!("Parsing GPX: {display}"))?;

        out.push(Workout {
            start,
            duration,
            source: format!("gpx:{file_name}"),
        });
    }

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
                    let dt = dt_fixed.with_timezone(&Utc);

                    min_t = Some(min_t.map_or(dt, |cur| cur.min(dt)));
                    max_t = Some(max_t.map_or(dt, |cur| cur.max(dt)));
                }
            }
            Err(_) => {
                // If this is a malformed GPX, treat duration as unknown rather than failing the whole run.
                return Ok(None);
            }
            _ => {}
        }

        buf.clear();
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

    let tables = list_tables(&conn)?;
    if tables.is_empty() {
        return Ok(Vec::new());
    }

    let mut best: Option<(i32, Vec<Workout>)> = None;

    for table in tables {
        let cols = table_columns(&conn, &table)?;
        let candidate = build_table_candidate(&table, &cols);
        let Some(cand) = candidate else {
            continue;
        };

        let workouts = extract_workouts_from_table(&conn, &cand)?;
        if workouts.len() < 3 {
            continue;
        }

        let len_i32 = i32::try_from(workouts.len()).unwrap_or(i32::MAX);
        let score = cand.score + (len_i32 * 3) + recency_bonus(&workouts);

        match &best {
            None => best = Some((score, workouts)),
            Some((best_score, _)) if score > *best_score => best = Some((score, workouts)),
            _ => {}
        }
    }

    Ok(best.map(|(_, w)| w).unwrap_or_default())
}

fn list_tables(conn: &Connection) -> Result<Vec<String>> {
    let mut stmt = conn.prepare(
        "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'",
    )?;
    let it = stmt.query_map([], |row| row.get::<_, String>(0))?;

    let mut out = Vec::new();
    for t in it {
        out.push(t?);
    }
    Ok(out)
}

fn table_columns(conn: &Connection, table: &str) -> Result<Vec<String>> {
    let sql = format!("PRAGMA table_info({})", quote_ident(table));
    let mut stmt = conn.prepare(&sql)?;
    let it = stmt.query_map([], |row| row.get::<_, String>(1))?;

    let mut cols = Vec::new();
    for c in it {
        cols.push(c?);
    }
    Ok(cols)
}

#[derive(Debug, Clone)]
struct TableCandidate {
    table: String,
    start_col: String,
    end_col: Option<String>,
    dur_col: Option<String>,
    type_col: Option<String>,
    score: i32,
}

fn build_table_candidate(table: &str, cols: &[String]) -> Option<TableCandidate> {
    let name_score = table_name_score(table);

    let start_col = best_col(cols, col_score_start)?;
    let end_col = best_col(cols, col_score_end);
    let dur_col = best_col(cols, col_score_duration);
    if end_col.is_none() && dur_col.is_none() {
        return None;
    }

    let type_col = best_col(cols, col_score_type);

    let mut score = name_score;
    score += col_score_start(&start_col);

    if let Some(ec) = &end_col {
        score += col_score_end(ec);
    }
    if let Some(dc) = &dur_col {
        score += col_score_duration(dc);
    }

    if score < 6 && name_score <= 0 {
        return None;
    }

    Some(TableCandidate {
        table: table.to_string(),
        start_col,
        end_col,
        dur_col,
        type_col,
        score,
    })
}

fn extract_workouts_from_table(conn: &Connection, cand: &TableCandidate) -> Result<Vec<Workout>> {
    let mut select_cols = vec![quote_ident(&cand.start_col)];

    if let Some(ec) = &cand.end_col {
        select_cols.push(quote_ident(ec));
    } else if let Some(dc) = &cand.dur_col {
        select_cols.push(quote_ident(dc));
    }

    if let Some(tc) = &cand.type_col {
        select_cols.push(quote_ident(tc));
    }

    let sql = format!(
        "SELECT {} FROM {} ORDER BY {} DESC LIMIT 500",
        select_cols.join(", "),
        quote_ident(&cand.table),
        quote_ident(&cand.start_col),
    );

    let mut stmt = conn.prepare(&sql)?;
    let mut rows = stmt.query([])?;

    let mut out = Vec::new();
    while let Some(row) = rows.next()? {
        if let Some(w) = row_to_workout(row, cand)? {
            out.push(w);
        }
    }

    out.sort_by(|a, b| b.start.cmp(&a.start));
    Ok(out)
}

fn row_to_workout(row: &Row<'_>, cand: &TableCandidate) -> Result<Option<Workout>> {
    let start_v = row.get_ref(0)?;
    let Some(start) = parse_datetime_value(start_v) else {
        return Ok(None);
    };

    let duration = if cand.end_col.is_some() {
        let end_v = row.get_ref(1)?;
        parse_datetime_value(end_v).and_then(|e| (e > start).then_some(e - start))
    } else {
        let dur_v = row.get_ref(1)?;
        parse_duration_value(dur_v)
    };

    if !plausible_start(&start) {
        return Ok(None);
    }
    if let Some(d) = duration
        && !plausible_duration(&d)
    {
        return Ok(None);
    }

    Ok(Some(Workout {
        start,
        duration,
        source: format!("db:{}", cand.table),
    }))
}

fn plausible_start(dt: &DateTime<Utc>) -> bool {
    let earliest = Utc.with_ymd_and_hms(2010, 1, 1, 0, 0, 0).unwrap();
    let latest = Utc::now() + Duration::days(1);
    *dt >= earliest && *dt <= latest
}

fn plausible_duration(d: &Duration) -> bool {
    let secs = d.num_seconds();
    (30..=24 * 60 * 60).contains(&secs)
}

fn parse_datetime_value(v: ValueRef<'_>) -> Option<DateTime<Utc>> {
    match v {
        ValueRef::Integer(i) => epoch_to_utc(i),
        ValueRef::Real(f) => f64_to_i64_trunc(f).and_then(epoch_to_utc),
        ValueRef::Text(t) => {
            let s = std::str::from_utf8(t).ok()?.trim();

            if let Ok(dt_fixed) = DateTime::parse_from_rfc3339(s) {
                return Some(dt_fixed.with_timezone(&Utc));
            }

            if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%d %H:%M:%S") {
                return Some(DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc));
            }
            if let Ok(ndt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
                return Some(DateTime::<Utc>::from_naive_utc_and_offset(ndt, Utc));
            }

            if let Ok(i) = s.parse::<i64>() {
                return epoch_to_utc(i);
            }

            None
        }
        ValueRef::Null | ValueRef::Blob(_) => None,
    }
}

fn f64_to_i64_trunc(f: f64) -> Option<i64> {
    if !f.is_finite() {
        return None;
    }
    let s = format!("{:.0}", f.trunc());
    s.parse::<i64>().ok()
}

fn epoch_to_utc(i: i64) -> Option<DateTime<Utc>> {
    if i <= 0 {
        return None;
    }

    let secs = if i > 1_000_000_000_000_000_000 {
        i / 1_000_000_000
    } else if i > 1_000_000_000_000_000 {
        i / 1_000_000
    } else if i > 1_000_000_000_000 {
        i / 1_000
    } else {
        i
    };

    Utc.timestamp_opt(secs, 0).single()
}

fn parse_duration_value(v: ValueRef<'_>) -> Option<Duration> {
    match v {
        ValueRef::Integer(i) => {
            if i < 0 {
                return None;
            }
            if i >= 1_000_000 {
                Some(Duration::milliseconds(i))
            } else {
                Some(Duration::seconds(i))
            }
        }
        ValueRef::Real(f) => {
            if !f.is_finite() || f.is_sign_negative() {
                return None;
            }
            let ms = f64_to_i64_trunc((f * 1000.0).round())?;
            Some(Duration::milliseconds(ms))
        }
        ValueRef::Text(t) => {
            let s = std::str::from_utf8(t).ok()?.trim();
            if let Ok(i) = s.parse::<i64>() {
                return parse_duration_value(ValueRef::Integer(i));
            }
            if let Ok(f) = s.parse::<f64>() {
                if f.is_sign_negative() {
                    return None;
                }
                let ms = f64_to_i64_trunc((f * 1000.0).round())?;
                return Some(Duration::milliseconds(ms));
            }
            None
        }
        ValueRef::Null | ValueRef::Blob(_) => None,
    }
}

fn recency_bonus(workouts: &[Workout]) -> i32 {
    let Some(max_start) = workouts.iter().map(|w| w.start).max() else {
        return 0;
    };

    let age = Utc::now().signed_duration_since(max_start).num_days();
    if age <= 7 {
        25
    } else if age <= 30 {
        10
    } else {
        0
    }
}

fn quote_ident(ident: &str) -> String {
    format!("\"{}\"", ident.replace('"', "\"\""))
}

// ---------------------------- Heuristics ---------------------------------

fn table_name_score(table: &str) -> i32 {
    let t = table.to_lowercase();
    let mut s = 0;

    for (pat, w) in [
        ("workout", 8),
        ("activity", 6),
        ("training", 6),
        ("sport", 5),
        ("session", 5),
        ("run", 3),
        ("exercise", 4),
        ("track", 2),
    ] {
        if t.contains(pat) {
            s += w;
        }
    }

    for (pat, w) in [
        ("sample", -4),
        ("samples", -4),
        ("raw", -3),
        ("debug", -4),
        ("log", -3),
    ] {
        if t.contains(pat) {
            s += w;
        }
    }

    s
}

fn best_col<F>(cols: &[String], score_fn: F) -> Option<String>
where
    F: Fn(&str) -> i32,
{
    let mut best: Option<(&String, i32)> = None;

    for c in cols {
        let sc = score_fn(c);
        if sc <= 0 {
            continue;
        }
        match best {
            None => best = Some((c, sc)),
            Some((_, best_sc)) if sc > best_sc => best = Some((c, sc)),
            _ => {}
        }
    }

    best.map(|(c, _)| c.clone())
}

fn col_score_start(col: &str) -> i32 {
    let c = col.to_lowercase();
    let mut s = 0;
    if c.contains("start") {
        s += 6;
    }
    if c.contains("begin") {
        s += 4;
    }
    if c.contains("time") {
        s += 3;
    }
    if c.contains("ts") || c.contains("timestamp") {
        s += 4;
    }
    if c == "timestamp" {
        s += 2;
    }
    s
}

fn col_score_end(col: &str) -> i32 {
    let c = col.to_lowercase();
    let mut s = 0;
    if c.contains("end") {
        s += 6;
    }
    if c.contains("stop") {
        s += 4;
    }
    if c.contains("finish") {
        s += 4;
    }
    if c.contains("time") {
        s += 3;
    }
    if c.contains("ts") || c.contains("timestamp") {
        s += 4;
    }
    s
}

fn col_score_duration(col: &str) -> i32 {
    let c = col.to_lowercase();
    let mut s = 0;
    if c.contains("duration") {
        s += 8;
    }
    if c.contains("elapsed") {
        s += 5;
    }
    if c.contains("total") && c.contains("time") {
        s += 5;
    }
    if c.contains("moving") && c.contains("time") {
        s += 4;
    }
    if c.ends_with("_ms") {
        s += 3;
    }
    if c.ends_with("_sec") || c.ends_with("_secs") {
        s += 3;
    }
    s
}

fn col_score_type(col: &str) -> i32 {
    let c = col.to_lowercase();
    let mut s = 0;
    if c == "type" {
        s += 5;
    }
    if c.contains("sport") {
        s += 5;
    }
    if c.contains("activity") {
        s += 4;
    }
    if c.contains("name") {
        s += 2;
    }
    s
}

// ---------------------------- Merge & formatting ---------------------------------

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
