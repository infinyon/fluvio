use fluvio_smartmodule::{smartstream, Record, RecordData, Result};

#[smartstream(join)]
pub fn my_join(_record: &Record, _record1: &Record) -> Result<(Option<RecordData>, RecordData)> {
    unimplemented!()
}

fn main() {}
