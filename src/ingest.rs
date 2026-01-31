use crate::database::read_base_activity_summary;
use crate::dlog;
use crate::gpx::parse_gpx_points;
use crate::types::{GpxPoint, WorkoutSummary};
use crate::utils::{duration_seconds_i32, e7_to_degrees, map_android_gpx_to_export};
use anyhow::{Context, Result};
use postgres::{Client, NoTls};
use std::path::Path;

pub fn ingest(
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
