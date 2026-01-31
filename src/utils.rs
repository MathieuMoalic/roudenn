use crate::types::Workout;
use chrono::{DateTime, Duration, FixedOffset, Utc};
use regex::Regex;
use std::collections::HashMap;
use std::path::Path;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicBool, Ordering};

static TS_RE: LazyLock<Regex> = LazyLock::new(|| {
    // Matches Gadgetbridge's filename timestamp style:
    // 2026-01-29T08_25_59+01_00  (underscores instead of colons)
    Regex::new(r"(\d{4}-\d{2}-\d{2}T\d{2}_\d{2}_\d{2}[+-]\d{2}_\d{2})").unwrap()
});

static DEBUG: AtomicBool = AtomicBool::new(false);

#[macro_export]
macro_rules! dlog {
    ($($arg:tt)*) => {
        if $crate::utils::is_debug() {
            eprintln!($($arg)*);
        }
    };
}

pub fn set_debug(enabled: bool) {
    DEBUG.store(enabled, Ordering::Relaxed);
}

pub fn is_debug() -> bool {
    DEBUG.load(Ordering::Relaxed)
}

pub fn parse_start_from_filename(file_name: &str) -> Option<DateTime<Utc>> {
    let caps = TS_RE.captures(file_name)?;
    let raw = caps.get(1)?.as_str();

    let rfc3339 = raw.replace('_', ":");
    let dt_fixed: DateTime<FixedOffset> = DateTime::parse_from_rfc3339(&rfc3339).ok()?;
    Some(dt_fixed.with_timezone(&Utc))
}

pub fn merge_by_start_minute(workouts: Vec<Workout>) -> Vec<Workout> {
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

pub fn format_duration(d: Duration) -> String {
    let secs = d.num_seconds().unsigned_abs();
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

pub fn map_android_gpx_to_export(
    export_dir: &Path,
    android_path: &str,
) -> Option<std::path::PathBuf> {
    let file_name = Path::new(android_path).file_name()?.to_str()?;
    Some(export_dir.join("files").join(file_name))
}

pub fn map_android_raw_details_to_export(
    export_dir: &Path,
    android_path: &str,
) -> Option<std::path::PathBuf> {
    let file_name = Path::new(android_path).file_name()?.to_str()?;
    Some(export_dir.join("files").join("rawDetails").join(file_name))
}

pub fn duration_seconds_i32(d: Duration) -> i32 {
    let secs = d.num_seconds().abs();
    i32::try_from(secs).unwrap_or(i32::MAX)
}

pub fn e7_to_degrees(lon_e7: Option<i64>, lat_e7: Option<i64>) -> (Option<f64>, Option<f64>) {
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
