use fluvio_smartmodule::{smartmodule, Record, RecordData, Result, eyre};

#[smartmodule(map)]
pub fn my_map(_record: &Record) -> Result<(Option<RecordData>, RecordData)> {
    unimplemented!()
}

#[smartmodule(look_back)]
pub fn my_look_back(record: &Record) -> Result<()> {
    Err(eyre!("some user defined error {record:?}"))
}

fn main() {}
