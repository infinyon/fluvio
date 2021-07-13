use fluvio_smartstream::{smartstream, Record, RecordData};
use regex::Regex;
use once_cell::sync::Lazy;

static SOCIAL_SECURITY_RE: Lazy<Regex> = Lazy::new(|| Regex::new(r"\d{3}-\d{2}-\d{4}").unwrap());

#[smartstream(map)]
pub fn map(record: &Record) -> (Option<RecordData>, RecordData) {
    let key = record.key.clone();

    let string_result = std::str::from_utf8(record.value.as_ref());
    let string = match string_result {
        Ok(s) => s,
        Err(_) => return (key, record.value.clone()),
    };

    let output = SOCIAL_SECURITY_RE
        .replace_all(string, "***-**-****")
        .to_string();

    (key, output.into())
}
