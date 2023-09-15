use colored::Colorize;

pub trait Notify {
    fn command(&self) -> &'static str;
    fn is_quiet(&self) -> bool;

    fn notify_info(&self, message: &str) {
        if !self.is_quiet() {
            println!("{}: {}", "info".blue().bold(), message);
        }
    }

    fn notify_success(&self, message: &str) {
        if !self.is_quiet() {
            println!("{}: {}", "done".green().bold(), message);
        }
    }

    fn notify_warning(&self, message: &str) {
        if !self.is_quiet() {
            println!("{}: {}", "warn".yellow().bold(), message);
        }
    }
}
