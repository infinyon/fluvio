use fluvio_smartstream::{smartstream, Record, RecordData};

#[smartstream(aggregate)]
pub fn aggregate(accumulator: RecordData, next: &Record) -> RecordData {
    let mut acc = String::from_utf8_lossy(accumulator.as_ref()).to_string();
    let next = String::from_utf8_lossy(next.value.as_ref()).to_string();
    acc.push_str(&next);
    acc.into()
}
