use fluvio_smartmodule::{smartmodule, SmartOpt, Record, Result};

#[derive(SmartOpt)]
pub struct FilterOpt{
    key: String
}

impl Default for FilterOpt{
    fn default() -> Self{
        Self{
            key: "a".to_string()
        }
    }
}

#[smartmodule(filter, params)]
pub fn filter(record: &Record, opt: &FilterOpt) -> Result<bool> {
    let string = std::str::from_utf8(record.value.as_ref())?;
    Ok(string.contains(&opt.key))
}
