use colored::Colorize;

#[derive(Copy, Clone, Debug)]
pub struct Notify {
    /// Whether to suppress all output
    quiet: bool,
}

impl Notify {
    pub fn new(quiet: bool) -> Self {
        Self { quiet }
    }

    pub fn info(&self, message: impl AsRef<str>) {
        if !self.quiet {
            println!("{}: {}", "info".blue().bold(), message.as_ref());
        }
    }

    pub fn done(&self, message: impl AsRef<str>) {
        if !self.quiet {
            println!("{}: {}", "done".green().bold(), message.as_ref());
        }
    }

    pub fn warn(&self, message: impl AsRef<str>) {
        if !self.quiet {
            println!("{}: {}", "warn".yellow().bold(), message.as_ref());
        }
    }

    pub fn help(&self, message: impl AsRef<str>) {
        if !self.quiet {
            println!("{}: {}", "help".purple().bold(), message.as_ref());
        }
    }
}
