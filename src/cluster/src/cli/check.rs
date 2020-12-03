use structopt::StructOpt;

use crate::ClusterChecker;
use crate::cli::ClusterCliError;
use crate::check::{CheckError, CheckResults};

#[derive(Debug, StructOpt)]
pub struct CheckOpt {}

impl CheckOpt {
    pub async fn process(self) -> Result<(), ClusterCliError> {
        use colored::*;
        println!("{}", "Running pre-startup checks...".bold());
        let check_results = ClusterChecker::run_preflight_checks().await;
        check_results.render_checks();
        check_results.render_next_steps();
        Ok(())
    }
}

// Impl is here so that it only gets compiled when `feature = "cli"` is on
impl CheckResults {
    /// Pretty-prints itself to the terminal
    pub fn render_checks(&self) {
        use colored::*;

        for result in self.0.iter() {
            match result {
                Ok(success) => {
                    println!("✅ {} {}", "ok:".bold().green(), success.green());
                }
                Err(e @ CheckError::AutoRecoverable(_)) => {
                    println!("❕ {} {}", "warning:".bold().yellow(), e);
                    println!(
                        "  💡 {} this may be fixed automatically during startup",
                        "note:".bold()
                    );
                }
                Err(e @ CheckError::Unrecoverable(_)) => {
                    // Print one layer of source error
                    let cause = match std::error::Error::source(e) {
                        Some(underlying) => format!(": {}", underlying),
                        None => "".to_string(),
                    };
                    let msg = format!("{}{}", e, cause);
                    println!("❌ {} {}", "failed:".bold().red(), msg.red());
                }
                Err(CheckError::AlreadyInstalled) => {
                    println!(
                        "💙 {} {}",
                        "note:".bold().bright_blue(),
                        "Fluvio is already running".bright_blue()
                    );
                }
            }
        }
    }

    /// Prints suggestions of next steps to the user.
    ///
    /// This is useful when the user ran `fluvio cluster check` and needs
    /// to do more to start up the cluster. It is not so useful when this
    /// `ClusterResults` was produced by already performing the startup.
    pub fn render_next_steps(&self) {
        use colored::*;

        let mut failures = 0;
        let mut warnings = 0;
        let mut installed = false;

        for result in self.0.iter() {
            match result {
                Ok(_) => (),
                Err(CheckError::AutoRecoverable(_)) => {
                    warnings += 1;
                }
                Err(CheckError::Unrecoverable(_)) => {
                    failures += 1;
                }
                Err(CheckError::AlreadyInstalled) => {
                    installed = true;
                }
            }
        }

        // Print a conclusion message based on warnings and failures
        match (failures, warnings) {
            (0, 0) if !installed => {
                println!("{}", "All checks passed!".bold());
                println!("You may proceed with cluster startup");
                println!(
                    "{}: run `fluvio cluster start`",
                    "next".bold().bright_blue()
                );
            }
            (0, _) if !installed => {
                println!("{}", "Some checks failed, but may be auto-corrected".bold());
                println!("You may proceed with cluster startup");
                println!(
                    "{}: run `fluvio cluster start`",
                    "next".bold().bright_blue()
                );
            }
            _ if !installed => {
                let s = if failures == 1 { "" } else { "s" };
                print!("{}", format!("{} check{s} failed:", failures, s = s).bold());

                let it = if failures == 1 { "it" } else { "them" };
                println!(
                    " Please correct {it} before continuing with cluster startup",
                    it = it
                );
            }
            _ => {
                println!("{}", "Fluvio is already running!".bold());
                println!(
                    "To reset the cluster, run `fluvio cluster delete` then `fluvio cluster start`"
                );
            }
        }
    }
}
