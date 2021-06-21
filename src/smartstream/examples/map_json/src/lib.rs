use fluvio_smartstream::{smartstream, Record, RecordData};

#[smartstream(map)]
pub fn map(record: &Record) -> (Option<RecordData>, RecordData) {
    let key = record.key().cloned();

    let json_result = serde_json::from_slice::<serde_json::Value>(record.value.as_ref());
    let json = match json_result {
        Ok(value) => value,
        Err(_) => return (key, record.value.clone().into()),
    };

    let yaml_result = serde_yaml::to_vec(&json);
    let yaml_bytes = match yaml_result {
        Ok(bytes) => bytes,
        Err(_) => return (key, record.value.clone().into()),
    };

    (key, yaml_bytes.into())
}
