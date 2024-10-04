use chrono::{DateTime, Utc};

const SIMPLE_DATE_FORMAT: &str = "%Y/%m/%d";
const EXACT_FORMAT: &str = "%Y-%m-%d %H:%M:%S %Z";

pub struct TimeElapsedFormatter {
    bot: DateTime<Utc>,
}

impl Default for TimeElapsedFormatter {
    fn default() -> Self {
        Self { bot: Utc::now() }
    }
}

impl TimeElapsedFormatter {
    pub fn new(bot: DateTime<Utc>) -> Self {
        Self { bot }
    }

    pub fn time_since_str(&self, date: DateTime<Utc>, exact: bool) -> String {
        let duration = self.bot.signed_duration_since(date);

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
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, Days, TimeZone, Timelike, Utc};

    use crate::time::TimeElapsedFormatter;

    fn december_17_1989_10_30_00() -> DateTime<Utc> {
        Utc.with_ymd_and_hms(1989, 12, 17, 10, 30, 0)
            .unwrap()
            .to_utc()
    }

    fn make_fixed_time_elapsed_fmtr() -> TimeElapsedFormatter {
        TimeElapsedFormatter::new(december_17_1989_10_30_00())
    }

    #[test]
    fn determines_exact_date() {
        let next_min = december_17_1989_10_30_00().minute() - 30;
        let date = december_17_1989_10_30_00()
            .with_minute(next_min)
            .expect("failed to set min");
        let formatted = make_fixed_time_elapsed_fmtr().time_since_str(date, true);

        assert_eq!(formatted, "1989-12-17 10:00:00 UTC");
    }

    #[test]
    fn simple_date_format_for_exceeding_a_week() {
        let date = december_17_1989_10_30_00()
            .checked_sub_days(Days::new(7))
            .expect("failed to sub days");
        let formatted = make_fixed_time_elapsed_fmtr().time_since_str(date, false);

        assert_eq!(formatted, "1989/12/10");
    }

    #[test]
    fn renders_days_passed() {
        let date = december_17_1989_10_30_00()
            .checked_sub_days(Days::new(2))
            .expect("failed to sub days");
        let formatted = make_fixed_time_elapsed_fmtr().time_since_str(date, false);

        assert_eq!(formatted, "2 days ago");
    }

    #[test]
    fn renders_yesterday() {
        let date = december_17_1989_10_30_00()
            .checked_sub_days(Days::new(1))
            .expect("failed to sub days");
        let formatted = make_fixed_time_elapsed_fmtr().time_since_str(date, false);

        assert_eq!(formatted, "Yesterday");
    }

    #[test]
    fn renders_hours() {
        let next_hour = december_17_1989_10_30_00().hour() - 8;
        let date = december_17_1989_10_30_00()
            .with_hour(next_hour)
            .expect("failed to set hour");
        let formatted = make_fixed_time_elapsed_fmtr().time_since_str(date, false);

        assert_eq!(formatted, "8 hours ago");
    }

    #[test]
    fn renders_minutes() {
        let next_min = december_17_1989_10_30_00().minute() - 30;
        let date = december_17_1989_10_30_00()
            .with_minute(next_min)
            .expect("failed to set min");
        let formatted = make_fixed_time_elapsed_fmtr().time_since_str(date, false);

        assert_eq!(formatted, "30 minutes ago");
    }

    #[test]
    fn renders_just_now() {
        let formatted = TimeElapsedFormatter::default().time_since_str(Utc::now(), false);

        assert_eq!(formatted, "Just now");
    }
}
