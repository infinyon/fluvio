use structopt::StructOpt;

use crate::{ClusterChecker, CheckStatuses, CheckStatus};
use crate::cli::ClusterCliError;
use crate::check::{CheckFailed, CheckResults, CheckResult, CheckSuggestion};

const ISSUE_URL: &str = "https://github.com/infinyon/fluvio/issues/new/choose";

#[derive(Debug, StructOpt)]
pub struct CheckOpt {}

impl CheckOpt {
    pub async fn process(self) -> Result<(), ClusterCliError> {
        use colored::*;
        println!("{}", "Running pre-startup checks...".bold());
        let check_results = ClusterChecker::run_preflight_checks().await;
        check_results.render_results();
        check_results.render_next_steps();
        Ok(())
    }
}

//
// The following Impls are here so that they only gets compiled when `feature = "cli"` is on
//

impl CheckStatus {
    /// Renders a single check status
    pub fn render(&self) {
        use colored::*;
        use crate::CheckStatus::*;

        match self {
            Pass(success) => {
                println!("✅ {} {}", "ok:".bold().green(), success.green());
            }
            Fail(e @ CheckFailed::AutoRecoverable(_)) => {
                println!("❕ {} {}", "warning:".bold().yellow(), e);
                println!(
                    "  💡 {} this may be fixed automatically during startup",
                    "note:".bold()
                );
            }
            Fail(CheckFailed::AlreadyInstalled) => {
                println!(
                    "💙 {} {}",
                    "note:".bold().bright_blue(),
                    "Fluvio is already running".bright_blue()
                );
            }
            Fail(e @ CheckFailed::Unrecoverable(_)) => {
                // Print one layer of source error
                let cause = match std::error::Error::source(e) {
                    Some(underlying) => format!(": {}", underlying),
                    None => "".to_string(),
                };
                let msg = format!("{}{}", e, cause);
                println!("❌ {} {}", "failed:".bold().red(), msg.red());
            }
        }

        if let Some(suggestion) = self.suggestion() {
            println!("  💡 {} {}", "suggestion:".bold().cyan(), suggestion,)
        }
    }
}

impl CheckStatuses {
    /// Renders all of the inner check statuses
    pub fn render_checks(&self) {
        for status in self.0.iter() {
            status.render();
        }
    }

    /// Renders a conclusion message based on the number of failures and warnings
    pub fn render_next_steps(&self) {
        use crate::CheckStatus::*;

        let mut installed = false;
        let mut failures = 0;
        let mut warnings = 0;

        for status in self.0.iter() {
            match status {
                Pass(_) => (),
                Fail(CheckFailed::AlreadyInstalled) => installed = true,
                Fail(CheckFailed::AutoRecoverable(_)) => warnings += 1,
                Fail(CheckFailed::Unrecoverable(_)) => failures += 1,
            }
        }

        Self::render_next_steps_impl(failures, warnings, installed);
    }

    fn render_next_steps_impl(failures: u32, warnings: u32, installed: bool) {
        use colored::*;

        // Print a conclusion message based on warnings and failures
        match (failures, warnings) {
            // No errors, failures, or warnings
            (0, 0) if !installed => {
                println!("{}", "All checks passed!".bold());
                println!("You may proceed with cluster startup");
                println!(
                    "{}: run `fluvio cluster start`",
                    "next".bold().bright_blue()
                );
            }
            // No errors or failures, but there are warnings
            (0, _) if !installed => {
                println!("{}", "Some checks failed, but may be auto-corrected".bold());
                println!("You may proceed with cluster startup");
                println!(
                    "{}: run `fluvio cluster start`",
                    "next".bold().bright_blue()
                );
            }
            // No errors, but there are check failures
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

impl CheckResults {
    /// Pretty-prints itself to the terminal
    pub fn render_results(&self) {
        for result in self.0.iter() {
            Self::render_result(result);
        }
    }

    fn render_result(result: &CheckResult) {
        use colored::*;

        match result {
            Ok(status) => status.render(),
            Err(e) => {
                // Print one layer of source error
                let cause = match std::error::Error::source(e) {
                    Some(underlying) => format!(": {}", underlying),
                    None => "".to_string(),
                };
                let msg = format!("{}{}", e, cause);
                println!("🚨 {} {}", "[ERROR]:".bold().red(), msg.red());
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
        use crate::CheckStatus::*;

        let mut errors = 0;
        let mut failures = 0;
        let mut warnings = 0;
        let mut installed = false;

        for result in self.0.iter() {
            match result {
                Ok(Pass(_)) => (),
                Ok(Fail(CheckFailed::AlreadyInstalled)) => installed = true,
                Ok(Fail(CheckFailed::Unrecoverable(_))) => failures += 1,
                Ok(Fail(CheckFailed::AutoRecoverable(_))) => warnings += 1,
                Err(_) => errors += 1,
            }
        }

        // There was one or more errors performing checks
        if !installed && errors > 0 {
            let s = if errors == 1 { "" } else { "s" };
            println!(
                "{}",
                format!("There was an error performing {} check{s}", errors, s = s).bold()
            );
            println!("If you believe this is incorrect, please consider filing a bug report at:");
            println!("  {}", ISSUE_URL.bold());
            return;
        }

        // Print a conclusion message based on warnings and failures
        CheckStatuses::render_next_steps_impl(failures, warnings, installed);
    }
}
