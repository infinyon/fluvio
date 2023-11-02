use fluvio_smartmodule::{smartmodule, SmartModuleRecord, RecordData, Result};

#[smartmodule(map)]
pub fn map(record: &SmartModuleRecord) -> Result<(Option<RecordData>, RecordData)> {
    let json = serde_json::from_slice::<serde_json::Value>(record.value.as_ref())?;
    let yaml_bytes = serde_yaml::to_string(&json)?.into_bytes();

    Ok((record.key().cloned(), yaml_bytes.into()))
}
