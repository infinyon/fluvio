use fluvio_smartstream::{smartstream, Record, RecordData, Result};

#[smartstream(flatmap)]
pub fn flatmap(record: &Record) -> Result<Vec<(Option<RecordData>, RecordData)>> {
    let array = serde_json::from_slice::<Vec<serde_json::Value>>(record.value.as_ref())?;
    let strings = array
        .into_iter()
        .map(|value| serde_json::to_string(&value))
        .collect::<core::result::Result<Vec<String>, _>>()?;
    let kvs = strings
        .into_iter()
        .map(|s| (None, RecordData::from(s)))
        .collect::<Vec<_>>();
    Ok(kvs)
}
