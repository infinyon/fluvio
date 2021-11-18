use fluvio_smartmodule::{smartmodule, Record, RecordData, Result};

#[smartmodule(map)]
pub fn my_map(_record: &Record) -> Result<(Option<RecordData>, RecordData)> {
    unimplemented!()
}

fn main() {}
