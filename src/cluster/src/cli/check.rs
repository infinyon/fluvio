use structopt::StructOpt;

use crate::ClusterChecker;
use crate::cli::ClusterCliError;
use crate::check::CheckError;

#[derive(Debug, StructOpt)]
pub struct CheckOpt {
    /// run pre-install checks
    #[structopt(long)]
    pre_install: bool,
}

impl CheckOpt {
    pub async fn process(self) -> Result<(), ClusterCliError> {
        use colored::*;

        println!("{}", "Running pre-install checks...".bold());
        let check_results = ClusterChecker::run_preflight_checks().await;

        let mut failures = 0;
        let mut warnings = 0;
        for result in check_results {
            match result {
                Ok(success) => {
                    let msg = format!("ok: {}", success);
                    println!("✔️  {}", msg.green());
                }
                Err(e @ CheckError::AutoRecoverable(_)) => {
                    let msg = format!("warn: {}", e);
                    println!("⚠️ {}", msg.yellow());
                    println!("  💡 {}: this may be fixed automatically during installation", "note".yellow());
                    warnings += 1;
                }
                Err(e @ CheckError::Unrecoverable(_)) => {
                    let msg = format!("failed: {}", e);
                    println!("❌ {}", msg.red());
                    failures += 1;
                }
            }
        }

        // Print a conclusion message based on warnings and failures
        match (failures, warnings) {
            (0, 0) => {
                println!("{}", "All checks passed!".bold());
                println!("You may proceed with cluster startup");
                println!("{}: run `fluvio cluster start`", "next".bold().bright_blue());
            }
            (0, _) => {
                println!("{}", "Some checks failed, but may be auto-corrected".bold());
                println!("You may proceed with cluster startup");
            }
            _ => {
                println!("{}", format!("{} checks failed", failures).bold());
                println!("Please correct them before continuing with cluster startup")
            }
        }

        Ok(())
    }
}
