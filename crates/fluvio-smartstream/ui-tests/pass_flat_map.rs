use fluvio_smartstream::{smartstream, Record, RecordData, Result};

#[smartstream(flat_map)]
pub fn my_flat_map(_record: &Record) -> Result<Vec<(Option<RecordData>, RecordData)>> {
    unimplemented!()
}

fn main() {}
