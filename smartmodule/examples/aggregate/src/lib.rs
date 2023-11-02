use fluvio_smartmodule::{smartmodule, Result, SmartModuleRecord, RecordData};

/// This aggregate concatenate accumulator and current value
/// values: "a","b"
//  accumulator: "1",
//  "1a","1ab"
#[smartmodule(aggregate)]
pub fn aggregate(accumulator: RecordData, current: &SmartModuleRecord) -> Result<RecordData> {
    let mut acc = String::from_utf8(accumulator.as_ref().to_vec())?;
    let next = std::str::from_utf8(current.value.as_ref())?;
    acc.push_str(next);
    Ok(acc.into())
}
