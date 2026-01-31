use crate::types::{Workout, WorkoutSummary};
use crate::{dlog, utils::map_android_raw_details_to_export};
use anyhow::{Context, Result};
use chrono::{TimeZone, Utc};
use rusqlite::Connection;
use serde_json::Value as JsonValue;
use std::fs;
use std::path::Path;

pub fn collect_from_db(export_dir: &Path) -> Result<Vec<Workout>> {
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

pub fn read_base_activity_summary(
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
