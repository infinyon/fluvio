use fluvio_smartmodule::{smartmodule, Record, RecordData, Result};

#[smartmodule(join)]
pub fn my_join(_record: &Record, _record1: &Record) -> Result<(Option<RecordData>, RecordData)> {
    unimplemented!()
}

fn main() {}
