use crate::types::{GpxPoint, Workout};
use crate::{dlog, utils::parse_start_from_filename};
use anyhow::{Context, Result};
use chrono::{DateTime, Duration, Utc};
use quick_xml::events::{BytesEnd, BytesStart, Event};
use quick_xml::reader::Reader;
use std::fs;
use std::io::{BufReader, Cursor};
use std::path::Path;
use walkdir::WalkDir;

pub fn collect_from_gpx(export_dir: &Path) -> Result<Vec<Workout>> {
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

pub fn parse_gpx_points(path: &Path) -> Result<Vec<GpxPoint>> {
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
