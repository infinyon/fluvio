use anyhow::Result;
use chrono::{DateTime, Utc};
use timeago::Formatter;

pub fn time_elapsed(date: DateTime<Utc>) -> Result<String> {
    let now = Utc::now();
    let time_fmt = Formatter::new();
    let dur = now.signed_duration_since(date).to_std()?;

    Ok(time_fmt.convert(dur))
}
