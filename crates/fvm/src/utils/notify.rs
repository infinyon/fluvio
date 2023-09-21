use colored::Colorize;

pub trait Notify {
    fn is_quiet(&self) -> bool;

    fn notify_fail<T: Into<String>>(&self, message: T) {
        if !self.is_quiet() {
            println!("{}: {}", "fail".red().bold(), message.into());
        }
    }

    fn notify_info<T: Into<String>>(&self, message: T) {
        if !self.is_quiet() {
            println!("{}: {}", "info".blue().bold(), message.into());
        }
    }

    fn notify_done<T: Into<String>>(&self, message: T) {
        if !self.is_quiet() {
            println!("{}: {}", "done".green().bold(), message.into());
        }
    }

    fn notify_warn<T: Into<String>>(&self, message: T) {
        if !self.is_quiet() {
            println!("{}: {}", "warn".yellow().bold(), message.into());
        }
    }

    fn notify_help<T: Into<String>>(&self, message: T) {
        if !self.is_quiet() {
            println!("{}: {}", "help".purple().bold(), message.into());
        }
    }
}
