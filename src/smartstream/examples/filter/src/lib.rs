use fluvio_smartstream::{smartstream, Record};

#[smartstream(filter)]
pub fn my_filter(record: &Record) -> bool {
    String::from_utf8_lossy(record.value.as_ref()).contains('a')
}
