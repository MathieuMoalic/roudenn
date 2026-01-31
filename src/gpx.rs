use crate::types::GpxPoint;
use anyhow::Result;
use chrono::{DateTime, Utc};
use quick_xml::events::{BytesEnd, BytesStart, Event};
use quick_xml::reader::Reader;
use std::fs;
use std::io::{BufReader, Cursor};
use std::path::Path;

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
