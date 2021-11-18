use fluvio_smartmodule::{smartmodule, Record, Result};
use regex::Regex;

#[smartmodule(filter)]
pub fn filter(record: &Record) -> Result<bool> {
    let string = std::str::from_utf8(record.value.as_ref())?;

    // Check whether the Record contains a Social Security number
    let social_security_regex = Regex::new(r"\d{3}-\d{2}-\d{4}").unwrap();
    let has_ss = social_security_regex.is_match(string);

    // Only accept records that _do not_ have social security numbers in them
    Ok(!has_ss)
}
