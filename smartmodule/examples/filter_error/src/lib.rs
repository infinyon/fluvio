use fluvio_smartmodule::{Record, Result, smartmodule};

#[smartmodule(filter)]
pub fn filter(_: &Record) -> Result<bool> {
    let _a: u32 = "dasdas".parse()?;
    Ok(true)
}