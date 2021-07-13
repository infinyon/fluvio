use fluvio_smartstream::{smartstream, Record, Result};

#[smartstream(filter)]
pub fn filter(record: &Record) -> Result<bool> {
    let string = std::str::from_utf8(record.value.as_ref())?;
    let int = string.parse::<i32>()?;
    Ok(int % 2 == 0)
}
