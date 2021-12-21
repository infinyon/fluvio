use fluvio_smartmodule::{smartmodule, Record, RecordData, Result};

#[smartmodule(aggregate)]
pub fn my_aggregate(_accumulator: RecordData, _record: &Record) -> Result<RecordData> {
    unimplemented!()
}

fn main() {}
