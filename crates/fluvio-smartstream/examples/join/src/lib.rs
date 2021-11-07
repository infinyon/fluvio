use fluvio_smartstream::{smartstream, Record, RecordData, Result};

#[smartstream(join)]
pub fn join(left_record: &Record, right_record: &Option<Record>) -> Result<(Option<RecordData>, RecordData)> {
    let left_value: i32 = std::str::from_utf8(left_record.value.as_ref())?.parse()?;
    let right_value: i32 = std::str::from_utf8(right_record.as_ref().unwrap().value.as_ref())?.parse()?;
    let value = left_value + right_value;

    Ok((None, value.to_string().into()))
}
