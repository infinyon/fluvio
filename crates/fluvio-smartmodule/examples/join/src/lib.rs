use fluvio_smartmodule::{smartmodule, Record, RecordData, Result};

#[smartmodule(join)]
pub fn join(left_record: &Record, right_record: &Record) -> Result<(Option<RecordData>, RecordData)> {
    let left_value: i32 = std::str::from_utf8(left_record.value.as_ref())?.parse()?;
    let right_value: i32 = std::str::from_utf8(right_record.value.as_ref())?.parse()?;
    let value = left_value + right_value;

    Ok((None, value.to_string().into()))
}
