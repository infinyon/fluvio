use fluvio_smartmodule::{smartmodule, SmartOpt, Result, Record, RecordData};

#[derive(Default, SmartOpt)]
pub struct AggOpt;

#[smartmodule(aggregate, params)]
pub fn aggregate(_accumulator: RecordData, _current: &Record, _opt: &AggOpt) -> Result<RecordData> {
    unimplemented!()
}

fn main() {}
