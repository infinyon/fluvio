use colored::Colorize;

pub trait Notify {
    fn command(&self) -> &'static str;

    fn notify_info(&self, message: &str) {
        println!("{}: {}", "info".blue().bold(), message);
    }

    fn notify_success(&self, message: &str) {
        println!("{}: {}", "done".green().bold(), message);
    }

    fn notify_warning(&self, message: &str) {
        println!("{}: {}", "warn".yellow().bold(), message);
    }
}
