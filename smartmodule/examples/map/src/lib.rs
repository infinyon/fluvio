use fluvio_smartmodule::{smartmodule, SmartModuleRecord, RecordData, Result};

#[smartmodule(map)]
pub fn map(record: &SmartModuleRecord) -> Result<(Option<RecordData>, RecordData)> {
    let key = record.key.clone();
    let mut value = Vec::from(record.value.as_ref());

    value.make_ascii_uppercase();
    Ok((key, value.into()))
}
