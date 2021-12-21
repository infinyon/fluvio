//! This example demonstrates how to do elegant error handling with SmartModules.
//!
//! The `fluvio_smartmodule::Result<T>` type allows you to bubble-up any
//! error that implements `std::error::Error` using the try (`?`) operator.
//!
//! When an error is returned from our `#[smartmodule(filter)]` function,
//! it is rendered as an error stack, meaning that all inner errors are
//! printed out in a nice chain. In this example, sending a record that
//! is not an integer results in the following error in the consumer:
//!
//! ```text
//! Error:
//!    0: Consumer Error
//!    1: Fluvio client error
//!    2: User SmartModule failed with the following error: Oops something went wrong
//!
//!       Caused by:
//!          0: Failed to parse int
//!          1: invalid digit found in string
//!
//!       Location:
//!           filter_odd/src/lib.rs:45:38
//! ```

use fluvio_smartmodule::{smartmodule, Record, Result};

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
pub fn filter(record: &Record) -> Result<bool> {
    let string = std::str::from_utf8(record.value.as_ref())?;
    let int = string
        .parse::<i32>()
        .map_err(FirstErrorWrapper::from)
        .map_err(SecondErrorWrapper::from)?;
    Ok(int % 2 == 0)
}
