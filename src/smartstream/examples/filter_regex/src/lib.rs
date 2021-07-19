use fluvio_smartstream::{smartstream, Record};
use regex::Regex;

#[smartstream(filter)]
pub fn filter(record: &Record) -> bool {
    let string_result = std::str::from_utf8(record.value.as_ref());
    let string = match string_result {
        Ok(s) => s,
        Err(_) => return false,
    };

    // Check whether the Record contains a Social Security number
    let social_security_regex = Regex::new(r"\d{3}-\d{2}-\d{4}").unwrap();
    let has_ss = social_security_regex.is_match(string);

    // Only accept records that _do not_ have social security numbers in them
    !has_ss
}
