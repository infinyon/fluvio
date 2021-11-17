use fluvio_smartmodule::{smartstream, SmartOpt, Record, Result};

#[derive(Default, SmartOpt)]
pub struct FilterOpt {
    attr1: String,
    attr2: Option<i32>,
    attr3: bool,
}

#[smartstream(filter, params)]
pub fn my_filter(_record: &Record, _opt: &FilterOpt) -> Result<bool> {
    unimplemented!()
}

fn main() {}
