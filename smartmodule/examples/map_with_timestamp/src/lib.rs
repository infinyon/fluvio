use fluvio_smartmodule::{smartmodule, Result, Record, RecordData};
use chrono::{LocalResult, TimeZone};
/// This maps each record to a string that contains the offset, timestamp, and value.
#[smartmodule(map)]
pub fn map(record: &Record) -> Result<(Option<RecordData>, RecordData)> {
    let offset = record.offset().to_string();
    let timestamp = match chrono::Utc.timestamp_millis_opt(record.timestamp()) {
        LocalResult::Single(dt) => dt.to_string(),
        _ => "unknown".to_string(),
    };
    let value = std::str::from_utf8(record.value.as_ref())?;
    Ok((
        record.key.clone(),
        format!("{}:{}:{}", offset, timestamp, value).into(),
    ))
}
