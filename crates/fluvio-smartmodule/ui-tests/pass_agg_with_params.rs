use fluvio_smartmodule::{smartstream, SmartOpt, Result, Record, RecordData};

#[derive(Default, SmartOpt)]
pub struct AggOpt;

#[smartstream(aggregate, params)]
pub fn aggregate(_accumulator: RecordData, _current: &Record, _opt: &AggOpt) -> Result<RecordData> {
    unimplemented!()
}

fn main() {}
