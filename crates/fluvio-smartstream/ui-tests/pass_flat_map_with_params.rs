use fluvio_smartstream::{smartstream, SmartOpt, Record, RecordData, Result};

#[derive(Default, SmartOpt)]
pub struct FlatOpt{
    key: String,
}

#[smartstream(flat_map, params)]
pub fn my_flat_map(_record: &Record, _opt: &FlatOpt) -> Result<Vec<(Option<RecordData>, RecordData)>> {
    unimplemented!()
}

fn main() {}
