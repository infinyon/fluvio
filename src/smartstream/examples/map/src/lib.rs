use fluvio_smartstream::{smartstream, Record, RecordData};

#[smartstream(map)]
pub fn map(record: &Record) -> (Option<RecordData>, RecordData) {
    let key = record.key.clone();
    let mut value = Vec::from(record.value.as_ref());

    value.make_ascii_uppercase();
    (key, value.into())
}
