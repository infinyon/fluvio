use fluvio_smartmodule::{smartmodule, Record, Result, dataplane::smartmodule::SmartModuleExtraParams};

#[smartmodule(filter)]
pub fn my_filter(_record: &Record) -> Result<bool> {
    unimplemented!()
}

#[smartmodule(look_back)]
pub fn my_look_back(_record: &Record) -> Result<()> {
    unimplemented!()
}

#[smartmodule(init)]
fn init(_params: SmartModuleExtraParams) -> Result<()> {
    unimplemented!()
}

fn main() {}
