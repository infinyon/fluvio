use fluvio_smartstream::{smartstream, Record, RecordData, Result};

#[smartstream(map)]
pub fn my_map(_record: &Record) -> Result<(Option<RecordData>, RecordData)> {
    unimplemented!()
}

fn main() {}
