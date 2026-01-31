use anyhow::{Context, Result, bail};
use chrono::Duration;
use std::fs::{self, File};
use std::io;
use std::path::{Path, PathBuf};
use tempfile::TempDir;
use tracing_subscriber::{EnvFilter, fmt};
use zip::ZipArchive;

#[macro_export]
macro_rules! dlog {
    ($($arg:tt)*) => {
        tracing::debug!($($arg)*);
    };
}

/// Initialize colorful logging.
///
/// Default level is INFO.
/// - `-v` => DEBUG
/// - `-vv` => TRACE
/// - `-q` => WARN
/// - `-qq` => ERROR
///
/// `RUST_LOG` overrides everything (e.g. `RUST_LOG=trace`).
pub fn init_logging(verbose: u8, quiet: u8) {
    let net = verbose as i8 - quiet as i8;
    let level = match net {
        i8::MIN..=-2 => "error",
        -1 => "warn",
        0 => "info",
        1 => "debug",
        2..=i8::MAX => "trace",
    };

    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new(format!("warn,roudenn={level}")));

    let show_src = matches!(level, "debug" | "trace");

    fmt()
        .with_env_filter(filter)
        .with_ansi(true)
        .with_timer(tracing_subscriber::fmt::time::ChronoLocal::rfc_3339())
        .with_target(true)
        .with_level(true)
        .with_file(show_src)
        .with_line_number(show_src)
        .compact()
        .init();
}

/// Handle that keeps a tempdir alive if we extracted a ZIP.
pub struct ExportHandle {
    dir: PathBuf,
    _tmp: Option<TempDir>,
}

impl ExportHandle {
    pub fn dir(&self) -> &Path {
        &self.dir
    }
}

/// Accepts either:
/// - a directory containing `files/`, `database/`, etc.
/// - a `.zip` file which we extract to a temp dir
pub fn open_export(path: &Path) -> Result<ExportHandle> {
    if path.is_dir() {
        tracing::info!(path = %path.display(), "using export directory");
        return Ok(ExportHandle {
            dir: path.to_path_buf(),
            _tmp: None,
        });
    }

    if path
        .extension()
        .and_then(|s| s.to_str())
        .map(|s| s.eq_ignore_ascii_case("zip"))
        != Some(true)
    {
        bail!(
            "Export path must be a directory or a .zip file: {}",
            path.display()
        );
    }

    let zip_file = File::open(path).with_context(|| format!("opening zip: {}", path.display()))?;
    let mut zip =
        ZipArchive::new(zip_file).with_context(|| format!("reading zip: {}", path.display()))?;

    let tmp = tempfile::tempdir().context("creating tempdir for export zip")?;
    tracing::info!(
        zip = %path.display(),
        tmp = %tmp.path().display(),
        entries = zip.len(),
        "extracting export zip"
    );

    for i in 0..zip.len() {
        let mut f = zip.by_index(i).context("reading zip entry")?;

        // Prevent Zip Slip / path traversal.
        let Some(rel) = f.enclosed_name() else {
            tracing::warn!(name = %f.name(), "skipping unsafe zip entry path");
            continue;
        };

        let out_path = tmp.path().join(&rel);

        if f.is_dir() {
            fs::create_dir_all(&out_path)
                .with_context(|| format!("creating dir: {}", out_path.display()))?;
            continue;
        }

        if let Some(parent) = out_path.parent() {
            fs::create_dir_all(parent)
                .with_context(|| format!("creating dir: {}", parent.display()))?;
        }

        let mut out = File::create(&out_path)
            .with_context(|| format!("creating file: {}", out_path.display()))?;
        io::copy(&mut f, &mut out)
            .with_context(|| format!("extracting file: {}", out_path.display()))?;
    }

    let mut root = tmp.path().to_path_buf();
    if !looks_like_export(&root) {
        // Common case: zip contains a single top-level dir.
        let mut dirs = Vec::new();
        for e in fs::read_dir(&root).context("reading extracted root dir")? {
            let e = e?;
            if e.file_type()?.is_dir() {
                dirs.push(e.path());
            }
        }

        if dirs.len() == 1 && looks_like_export(&dirs[0]) {
            root = dirs.pop().unwrap();
        } else {
            bail!(
                "ZIP extracted but doesn't look like a Gadgetbridge export root: {}",
                tmp.path().display()
            );
        }
    }

    tracing::info!(export_root = %root.display(), "export ready");

    Ok(ExportHandle {
        dir: root,
        _tmp: Some(tmp),
    })
}

fn looks_like_export(dir: &Path) -> bool {
    dir.join("files").is_dir()
        || dir.join("database").is_dir()
        || dir.join("gadgetbridge.json").is_file()
        || dir.join("database").join("Gadgetbridge").is_file()
}

pub fn format_duration(d: Duration) -> String {
    let secs = d.num_seconds().unsigned_abs();
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{h:02}:{m:02}:{s:02}")
}

pub fn map_android_gpx_to_export(export_dir: &Path, android_path: &str) -> Option<PathBuf> {
    let file_name = Path::new(android_path).file_name()?.to_str()?;
    Some(export_dir.join("files").join(file_name))
}

pub fn map_android_raw_details_to_export(export_dir: &Path, android_path: &str) -> Option<PathBuf> {
    let file_name = Path::new(android_path).file_name()?.to_str()?;
    Some(export_dir.join("files").join("rawDetails").join(file_name))
}

pub fn duration_seconds_i32(d: Duration) -> i32 {
    let secs = d.num_seconds().abs();
    i32::try_from(secs).unwrap_or(i32::MAX)
}

pub fn e7_to_degrees(lon_e7: Option<i64>, lat_e7: Option<i64>) -> (Option<f64>, Option<f64>) {
    let denom = 10_000_000.0_f64;

    let lon = lon_e7
        .and_then(|v| i32::try_from(v).ok())
        .map(|v| f64::from(v) / denom);

    let lat = lat_e7
        .and_then(|v| i32::try_from(v).ok())
        .map(|v| f64::from(v) / denom);

    (lon, lat)
}
