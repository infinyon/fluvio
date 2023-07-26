use std::sync::atomic::{AtomicI32, Ordering::SeqCst};

use fluvio_smartmodule::{smartmodule, SmartModuleRecord, Result};

static PREV: AtomicI32 = AtomicI32::new(0);

#[smartmodule(filter)]
pub fn filter(record: &SmartModuleRecord) -> Result<bool> {
    let string = std::str::from_utf8(record.value.as_ref())?;
    let current: i32 = string.parse()?;
    let last = PREV.load(SeqCst);
    if current > last {
        PREV.store(current, SeqCst);
        Ok(true)
    } else {
        Ok(false)
    }
}

#[smartmodule(look_back)]
pub fn look_back(record: &SmartModuleRecord) -> Result<()> {
    let string = std::str::from_utf8(record.value.as_ref())?;
    let last: i32 = string.parse()?;
    PREV.store(last, SeqCst);
    Ok(())
}
