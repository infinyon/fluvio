use fluvio_smartmodule::{smartmodule, Record, Result};

#[smartmodule(filter)]
pub fn my_filter(_record: &Record) -> Result<bool> {
    unimplemented!()
}

fn main() {}
