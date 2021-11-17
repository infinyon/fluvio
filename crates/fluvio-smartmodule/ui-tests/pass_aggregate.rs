use fluvio_smartmodule::{smartstream, Record, RecordData, Result};

#[smartstream(aggregate)]
pub fn my_aggregate(_accumulator: RecordData, _record: &Record) -> Result<RecordData> {
    unimplemented!()
}

fn main() {}
