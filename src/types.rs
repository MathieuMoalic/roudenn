use chrono::{DateTime, Duration, Utc};
use serde_json::Value as JsonValue;

#[derive(Debug, Clone)]
pub struct Workout {
    pub start: DateTime<Utc>,
    pub duration: Option<Duration>,
    pub source: String,
}

#[derive(Debug, Clone)]
pub struct WorkoutSummary {
    pub name: Option<String>,
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
    pub activity_kind: i32,

    pub base_longitude_e7: Option<i64>,
    pub base_latitude_e7: Option<i64>,
    pub base_altitude: Option<i64>,

    pub gpx_track_android: Option<String>,
    pub raw_details_android: Option<String>,

    pub device_id: i32,
    pub user_id: i32,

    pub summary_data_raw: Option<String>,
    pub summary_data_json: Option<JsonValue>,
    pub raw_summary_data: Option<Vec<u8>>,

    pub raw_details: Option<Vec<u8>>,
}

#[derive(Debug, Clone)]
pub struct GpxPoint {
    pub idx: i32,
    pub t: DateTime<Utc>,
    pub lat: f64,
    pub lon: f64,
    pub ele: Option<f64>,
}
