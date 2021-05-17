use fluvio_smartstream::{smartstream, Record, RecordData};

#[smartstream(map)]
pub fn my_map(mut record: Record) -> Record {
    let mut value = Vec::from(record.value.as_ref());
    value.make_ascii_uppercase();
    record.value = RecordData::from(value);
    record
}
