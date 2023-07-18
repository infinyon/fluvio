use chrono::{LocalResult, TimeZone};

use fluvio_smartmodule::{smartmodule, Result, SmartModuleRecord, RecordData};

/// This aggregate concatenate accumulator and value with base timestamp
///
/// values: "a","b"
///  accumulator: "1",
///
///  "1a_[timestamp]","1ab_[timestamp]"
#[smartmodule(aggregate)]
pub fn aggregate(accumulator: RecordData, record: &SmartModuleRecord) -> Result<RecordData> {
    let mut acc = String::from_utf8(accumulator.as_ref().to_vec())?;
    let next = std::str::from_utf8(record.value.as_ref())?;
    let timestamp = match chrono::Utc.timestamp_millis_opt(record.timestamp()) {
        LocalResult::Single(dt) => dt.to_string(),
        _ => "unknown".to_string(),
    };

    acc.push_str(format!("{}_[{}]", next, timestamp).as_str());

    Ok(acc.into())
}
