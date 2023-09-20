use colored::Colorize;

pub trait Notify {
    fn command(&self) -> &'static str;
    fn is_quiet(&self) -> bool;

    fn notify_info<T: Into<String>>(&self, message: T) {
        if !self.is_quiet() {
            println!("{}: {}", "info".blue().bold(), message.into());
        }
    }

    fn notify_success<T: Into<String>>(&self, message: T) {
        if !self.is_quiet() {
            println!("{}: {}", "done".green().bold(), message.into());
        }
    }

    fn notify_warning<T: Into<String>>(&self, message: T) {
        if !self.is_quiet() {
            println!("{}: {}", "warn".yellow().bold(), message.into());
        }
    }
}
