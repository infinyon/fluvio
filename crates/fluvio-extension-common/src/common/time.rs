use chrono::{DateTime, Utc};

const SIMPLE_DATE_FORMAT: &str = "%Y/%m/%d";
const EXACT_FORMAT: &str = "%Y-%m-%d %H:%M:%S %Z";

pub fn format_duration(date: DateTime<Utc>, exact: bool) -> String {
    let duration = Utc::now().signed_duration_since(date);

    if exact {
        return date.format(EXACT_FORMAT).to_string();
    }

    if duration.num_weeks() >= 1 {
        return date.format(SIMPLE_DATE_FORMAT).to_string();
    }

    if duration.num_days() >= 2 {
        return format!("{} days ago", duration.num_days());
    }

    if duration.num_days() == 1 {
        return String::from("Yesterday");
    }

    if duration.num_hours() >= 1 && duration.num_hours() < 24 {
        return format!("{} hours ago", duration.num_hours());
    }

    if duration.num_minutes() >= 1 && duration.num_minutes() < 59 {
        return format!("{} minutes ago", duration.num_minutes());
    }

    if duration.num_minutes() < 1 {
        return String::from("Just now");
    }

    date.format(SIMPLE_DATE_FORMAT).to_string()
}

#[cfg(test)]
mod tests {
    use chrono::{Days, Timelike, Utc};

    use crate::time::{EXACT_FORMAT, SIMPLE_DATE_FORMAT};

    use super::format_duration;

    #[test]
    fn determines_exact_date() {
        let date = Utc::now();
        let formatted = format_duration(date, true);

        assert_eq!(formatted, date.format(EXACT_FORMAT).to_string());
    }

    #[test]
    fn simple_date_format_for_exceeding_a_week() {
        let date = Utc::now()
            .checked_sub_days(Days::new(7))
            .expect("failed to sub days");
        let formatted = format_duration(date, false);

        assert_eq!(formatted, date.format(SIMPLE_DATE_FORMAT).to_string());
    }

    #[test]
    fn renders_days_passed() {
        let date = Utc::now()
            .checked_sub_days(Days::new(2))
            .expect("failed to sub days");
        let formatted = format_duration(date, false);

        assert_eq!(formatted, "2 days ago");
    }

    #[test]
    fn renders_yesterday() {
        let date = Utc::now()
            .checked_sub_days(Days::new(1))
            .expect("failed to sub days");
        let formatted = format_duration(date, false);

        assert_eq!(formatted, "Yesterday");
    }

    #[test]
    fn renders_hours() {
        let next_hour = Utc::now().hour() - 8;
        let date = Utc::now().with_hour(next_hour).expect("failed to set hour");
        let formatted = format_duration(date, false);

        assert_eq!(formatted, "8 hours ago");
    }

    #[test]
    fn renders_minutes() {
        let next_min = Utc::now().minute() - 30;
        let date = Utc::now().with_minute(next_min).expect("failed to set min");
        let formatted = format_duration(date, false);

        assert_eq!(formatted, "30 minutes ago");
    }

    #[test]
    fn renders_just_now() {
        let formatted = format_duration(Utc::now(), false);

        assert_eq!(formatted, "Just now");
    }
}
