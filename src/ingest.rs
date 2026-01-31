use crate::database::read_base_activity_summary;
use crate::dlog;
use crate::gpx::parse_gpx_points;
use crate::types::{GpxPoint, WorkoutSummary};
use crate::utils::{duration_seconds_i32, e7_to_degrees, map_android_gpx_to_export};
use anyhow::{Context, Result, bail};
use postgres::{Client, NoTls};
use std::path::Path;

pub fn ingest(export_dir: &Path, pg_url: &str) -> Result<()> {
    // Always store raw details + import points now.
    let store_raw_details = true;
    let with_points = true;

    let mut pg = connect_or_create_db(pg_url)?;
    ensure_pg_schema(&mut pg)?;

    let summaries = read_base_activity_summary(export_dir, store_raw_details)?;
    let total = summaries.len();
    tracing::info!(summaries = total, "found workouts");

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
                tracing::warn!(path = %gpx_path.display(), "gpx file referenced by db is missing");
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
    refresh_workout_distance_matview(&mut pg)?;
    tracing::info!(
        workouts_upserted = inserted_or_updated,
        workouts_with_points = workouts_with_points,
        points_imported = points_imported,
        "ingest done"
    );

    Ok(())
}

/// Connect to pg_url. If the database in the URL doesn't exist, create it and retry.
///
/// This requires privileges to CREATE DATABASE.
fn connect_or_create_db(pg_url: &str) -> Result<Client> {
    match Client::connect(pg_url, NoTls) {
        Ok(pg) => return Ok(pg),
        Err(e) => {
            if is_db_missing(&e) {
                // continue below
                tracing::warn!(err = %e, "database does not exist; attempting to create it");
            } else {
                return Err(e).context("Connecting to PostgreSQL");
            }
        }
    }

    let (db_name, admin_url_postgres, admin_url_template1) = admin_urls_for_create_db(pg_url)?;

    let mut admin = Client::connect(&admin_url_postgres, NoTls)
        .or_else(|_| Client::connect(&admin_url_template1, NoTls))
        .context("Connecting to maintenance DB (postgres/template1) to create target DB")?;

    if !database_exists(&mut admin, &db_name)? {
        tracing::info!(db = %db_name, "creating database");
        create_database(&mut admin, &db_name)?;
    } else {
        tracing::info!(db = %db_name, "database already exists");
    }

    Client::connect(pg_url, NoTls).context("Connecting to PostgreSQL after creating database")
}

fn is_db_missing(e: &postgres::Error) -> bool {
    e.as_db_error()
        .map(|d| d.code().code() == "3D000") // invalid_catalog_name (db does not exist)
        .unwrap_or(false)
}

fn database_exists(pg: &mut Client, db_name: &str) -> Result<bool> {
    Ok(pg
        .query_opt("SELECT 1 FROM pg_database WHERE datname = $1", &[&db_name])?
        .is_some())
}

fn create_database(pg: &mut Client, db_name: &str) -> Result<()> {
    // Avoid SQL injection: only allow simple identifiers.
    if db_name.is_empty()
        || !db_name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
    {
        bail!("Refusing to create database with unsafe name: {db_name:?}");
    }

    // CREATE DATABASE has no IF NOT EXISTS, so we check first; still handle race.
    let sql = format!("CREATE DATABASE \"{db_name}\"");
    match pg.batch_execute(&sql) {
        Ok(()) => Ok(()),
        Err(e) => {
            // 42P04 = duplicate_database
            if e.as_db_error()
                .map(|d| d.code().code() == "42P04")
                .unwrap_or(false)
            {
                Ok(())
            } else {
                Err(e).context("Creating database")
            }
        }
    }
}

/// Returns (dbname, admin_url_postgres, admin_url_template1).
///
/// Supports URI-style URLs like:
/// postgres://127.0.0.1:5432/fitness?sslmode=disable
fn admin_urls_for_create_db(pg_url: &str) -> Result<(String, String, String)> {
    let (base, query) = match pg_url.split_once('?') {
        Some((a, b)) => (a, Some(b)),
        None => (pg_url, None),
    };

    let slash = base
        .rfind('/')
        .context("pg_url must include a database name (e.g. .../fitness)")?;
    let db_name = &base[slash + 1..];
    if db_name.is_empty() {
        bail!("pg_url must include a database name (e.g. .../fitness)");
    }

    let prefix = &base[..slash + 1]; // keep trailing '/'

    let mut admin_postgres = format!("{prefix}postgres");
    let mut admin_template1 = format!("{prefix}template1");

    if let Some(q) = query {
        admin_postgres.push('?');
        admin_postgres.push_str(q);
        admin_template1.push('?');
        admin_template1.push_str(q);
    }

    Ok((db_name.to_string(), admin_postgres, admin_template1))
}
fn ensure_workout_distance_matview(pg: &mut Client) -> Result<()> {
    // Does the materialized view already exist?
    let exists = pg
        .query_opt(
            r#"
            SELECT 1
            FROM pg_matviews
            WHERE schemaname = 'public'
              AND matviewname = 'workout_distance_m'
            "#,
            &[],
        )
        .context("Checking for materialized view public.workout_distance_m")?
        .is_some();

    if exists {
        tracing::info!("materialized view workout_distance_m already exists");
        return Ok(());
    }

    tracing::info!("creating materialized view workout_distance_m");

    // Compute per-workout distance (meters) by summing haversine distances between consecutive points.
    pg.batch_execute(
        r#"
        CREATE MATERIALIZED VIEW public.workout_distance_m AS
        WITH p AS (
          SELECT
            workout_id,
            idx,
            lat,
            lon,
            LAG(lat) OVER (PARTITION BY workout_id ORDER BY idx) AS lat0,
            LAG(lon) OVER (PARTITION BY workout_id ORDER BY idx) AS lon0
          FROM public.workout_points
        ),
        seg AS (
          SELECT
            workout_id,
            2.0 * 6371000.0 * asin(
              sqrt(
                power(sin(radians(lat - lat0) / 2.0), 2)
                + cos(radians(lat0)) * cos(radians(lat))
                  * power(sin(radians(lon - lon0) / 2.0), 2)
              )
            ) AS dist_m
          FROM p
          WHERE lat0 IS NOT NULL AND lon0 IS NOT NULL
        )
        SELECT
          workout_id,
          SUM(dist_m) AS distance_m
        FROM seg
        GROUP BY workout_id;
        "#,
    )
    .context("Creating materialized view public.workout_distance_m")?;

    // Required for REFRESH ... CONCURRENTLY and also useful for joins.
    pg.batch_execute(
        r#"
        CREATE UNIQUE INDEX workout_distance_m_workout_id_idx
          ON public.workout_distance_m (workout_id);
        "#,
    )
    .context("Creating unique index on public.workout_distance_m")?;

    Ok(())
}

fn refresh_workout_distance_matview(pg: &mut Client) -> Result<()> {
    // Concurrent refresh avoids blocking reads in Grafana.
    // NOTE: This must not run inside an explicit transaction.
    pg.batch_execute("REFRESH MATERIALIZED VIEW CONCURRENTLY public.workout_distance_m;")
        .context("Refreshing materialized view public.workout_distance_m")?;
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
    ensure_workout_distance_matview(pg)?;

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

    Ok(row.get(0))
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
