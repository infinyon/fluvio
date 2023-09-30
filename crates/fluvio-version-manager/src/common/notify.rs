use colored::Colorize;

pub trait Notify {
    fn is_quiet(&self) -> bool;

    fn notify_info(&self, message: impl AsRef<str>) {
        if !self.is_quiet() {
            println!("{}: {}", "info".blue().bold(), message.as_ref());
        }
    }

    fn notify_done(&self, message: impl AsRef<str>) {
        if !self.is_quiet() {
            println!("{}: {}", "done".green().bold(), message.as_ref());
        }
    }

    fn notify_warn(&self, message: impl AsRef<str>) {
        if !self.is_quiet() {
            println!("{}: {}", "warn".yellow().bold(), message.as_ref());
        }
    }

    fn notify_help(&self, message: impl AsRef<str>) {
        if !self.is_quiet() {
            println!("{}: {}", "help".purple().bold(), message.as_ref());
        }
    }
}
