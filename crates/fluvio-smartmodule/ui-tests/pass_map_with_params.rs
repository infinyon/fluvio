use fluvio_smartmodule::{smartmodule, SmartOpt, Record, RecordData, Result};

#[derive(Default, SmartOpt)]
pub struct MapOpt;

#[smartmodule(map, params)]
pub fn map(_record: &Record, _opt: &MapOpt) -> Result<(Option<RecordData>, RecordData)> {
    unimplemented!()
}

fn main() {}
