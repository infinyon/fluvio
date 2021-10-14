use fluvio_smartstream::{smartstream, SmartOpt, Record, RecordData, Result};

#[derive(Default, SmartOpt)]
pub struct MapOpt;

#[smartstream(map, params)]
pub fn map(_record: &Record, _opt: &MapOpt) -> Result<(Option<RecordData>, RecordData)> {
    unimplemented!()
}

fn main() {}
