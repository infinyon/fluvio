use regex::Regex;
use once_cell::sync::Lazy;
use fluvio_smartmodule::{smartmodule, Result, Record, RecordData};

static SSN_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\d{3}-\d{2}-\d{4}").unwrap());

#[smartmodule(map)]
pub fn map(record: &Record) -> Result<(Option<RecordData>, RecordData)> {
    let key = record.key.clone();

    let string = std::str::from_utf8(record.value.as_ref())?;
    let output = SSN_RE.replace_all(string, "***-**-****").to_string();

    Ok((key, output.into()))
}
