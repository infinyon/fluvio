use fluvio_smartstream::{smartstream, Record, RecordData};

#[smartstream(map)]
pub fn map(mut record: Record) -> Record {
    let json_result = serde_json::from_slice::<serde_json::Value>(record.value.as_ref());
    let json = match json_result {
        Ok(value) => value,
        Err(_) => return record,
    };

    let yaml_result = serde_yaml::to_vec(&json);
    let yaml_bytes = match yaml_result {
        Ok(bytes) => bytes,
        Err(_) => return record,
    };

    record.value = RecordData::from(yaml_bytes);
    record
}
