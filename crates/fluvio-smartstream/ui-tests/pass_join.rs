use fluvio_smartstream::{smartstream, Record, RecordData, Result};

#[smartstream(join)]
pub fn my_join(_record: &Record, _record1: &Option<Record>) -> Result<(Option<RecordData>, RecordData)> {
    unimplemented!()
}

fn main() {}
