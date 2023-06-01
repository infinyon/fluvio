use fluvio_smartmodule::{smartmodule, SmartOpt, Record, RecordData, Result};

#[derive(Default, SmartOpt)]
pub struct MapOpt;

#[smartmodule(map, params)]
pub fn map(_record: &Record, _opt: &MapOpt) -> Result<(Option<RecordData>, RecordData)> {
    unimplemented!()
}

#[smartmodule(look_back)]
pub fn my_look_back(_record: &Record) -> Result<()> {
    unimplemented!()
}

fn main() {}
