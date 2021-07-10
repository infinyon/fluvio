use fluvio_smartstream::{smartstream, Record, RecordData, Result};

#[smartstream(map)]
pub fn map(record: &Record) -> Result<(Option<RecordData>, RecordData)> {
    let json = serde_json::from_slice::<serde_json::Value>(record.value.as_ref())?;
    let yaml_bytes = serde_yaml::to_vec(&json)?;

    Ok((record.key().cloned(), yaml_bytes.into()))
}
