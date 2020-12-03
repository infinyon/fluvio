use structopt::StructOpt;

use crate::ClusterChecker;
use crate::cli::ClusterCliError;
use crate::check::{CheckError, CheckResults};

#[derive(Debug, StructOpt)]
pub struct CheckOpt {}

impl CheckOpt {
    pub async fn process(self) -> Result<(), ClusterCliError> {
        use colored::*;
        println!("{}", "Running pre-install checks...".bold());
        let check_results = ClusterChecker::run_preflight_checks().await;
        check_results.render();
        Ok(())
    }
}

// This impl is here so it is only compiled when the "cli" feature is enabled
impl CheckResults {
    /// Pretty-prints itself to the terminal
    pub fn render(&self) {
        use colored::*;

        let mut failures = 0;
        let mut warnings = 0;
        let mut installed = false;

        for result in self.0.iter() {
            match result {
                Ok(success) => {
                    let msg = format!("ok: {}", success);
                    println!("✅ {}", msg.green());
                }
                Err(e @ CheckError::AutoRecoverable(_)) => {
                    let msg = format!("warn: {}", e);
                    println!("⚠️ {}", msg.yellow());
                    println!("  💡 {}: this may be fixed automatically during startup", "note".yellow());
                    warnings += 1;
                }
                Err(e @ CheckError::Unrecoverable(_)) => {
                    // Print one layer of source error
                    let cause = match std::error::Error::source(e) {
                        Some(underlying) => format!(": {}", underlying),
                        None => "".to_string(),
                    };
                    let msg = format!("failed: {}{}", e, cause);
                    println!("❌ {}", msg.red());
                    failures += 1;
                }
                Err(CheckError::AlreadyInstalled) => {
                    println!("{}", "💙 note: Fluvio is already running".bright_blue());
                    installed = true;
                }
            }
        }

        // Print a conclusion message based on warnings and failures
        match (failures, warnings) {
            (0, 0) if !installed => {
                println!("{}", "All checks passed!".bold());
                println!("You may proceed with cluster startup");
                println!("{}: run `fluvio cluster start`", "next".bold().bright_blue());
            }
            (0, _) if !installed => {
                println!("{}", "Some checks failed, but may be auto-corrected".bold());
                println!("You may proceed with cluster startup");
            }
            _ if !installed => {
                let s = if failures == 1 { "" } else { "s" };
                print!("{}", format!("{} check{s} failed:", failures, s = s).bold());

                let it = if failures == 1 { "it" } else { "them" };
                println!(" Please correct {it} before continuing with cluster startup", it = it);
            }
            _ => {
                println!("{}", "Fluvio is already running!".bold());
                println!("To reset the cluster, run `fluvio cluster delete` then `fluvio cluster start`");
            }
        }
    }
}
