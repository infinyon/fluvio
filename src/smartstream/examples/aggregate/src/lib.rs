use fluvio_smartstream::{smartstream, Result, Record, RecordData};

#[smartstream(aggregate)]
pub fn aggregate(accumulator: RecordData, next: &Record) -> Result<RecordData> {
    let mut acc = String::from_utf8(accumulator.as_ref().to_vec())?;
    let next = std::str::from_utf8(next.value.as_ref())?;
    acc.push_str(next);
    Ok(acc.into())
}
