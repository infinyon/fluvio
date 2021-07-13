use fluvio_smartstream::{smartstream, Record, RecordData, Result};

#[smartstream(map)]
pub fn map(record: &Record) -> Result<(Option<RecordData>, RecordData)> {
    let key = record.key.clone();
    let mut value = Vec::from(record.value.as_ref());

    value.make_ascii_uppercase();
    Ok((key, value.into()))
}
