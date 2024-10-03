use chrono::{DateTime, Utc};

pub fn format_duration(date: DateTime<Utc>, exact: bool) -> String {
    let duration = Utc::now().signed_duration_since(date);

    if exact {
        return date.format("%Y-%m-%d %H:%M:%S %Z").to_string();
    }

    if duration.num_weeks() >= 1 {
        return date.format("%Y/%m/%d").to_string();
    }

    if duration.num_days() >= 2 {
        return format!("{} days ago", duration.num_days());
    }

    if duration.num_days() == 1 {
        return String::from("Yesterday");
    }

    if duration.num_hours() >= 1 {
        return format!("{} hours ago", duration.num_hours());
    }

    if duration.num_minutes() >= 1 {
        return format!("{} minutes ago", duration.num_minutes());
    }

    if duration.num_minutes() < 1 {
        return String::from("Just now");
    }

    date.format("%Y/%m/%d").to_string()
}
