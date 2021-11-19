use fluvio_smartmodule::{smartmodule, Record, RecordData, Result};

#[smartmodule(array_map)]
pub fn my_array_map(_record: &Record) -> Result<Vec<(Option<RecordData>, RecordData)>> {
    unimplemented!()
}

fn main() {}
