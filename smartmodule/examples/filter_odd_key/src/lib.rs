//! This example demonstrates how to use key-value pairs

use fluvio_smartmodule::{smartmodule, SmartModuleRecord, Result};

#[derive(Debug, thiserror::Error)]
pub enum SecondErrorWrapper {
    #[error("Oops something went wrong")]
    FirstError(#[from] FirstErrorWrapper),
}

#[derive(Debug, thiserror::Error)]
pub enum FirstErrorWrapper {
    #[error("Failed to parse int")]
    ParseIntError(#[from] std::num::ParseIntError),
}

#[smartmodule(filter)]
pub fn filter(record: &SmartModuleRecord) -> Result<bool> {
    let message_key = match &record.key {
        Some(key_data) => std::str::from_utf8(key_data.as_ref())?,
        None => "", // or some default behavior
    };

    let int = message_key
        .parse::<i32>()
        .map_err(FirstErrorWrapper::from)
        .map_err(SecondErrorWrapper::from)?;
    Ok(int % 2 == 0)
}
