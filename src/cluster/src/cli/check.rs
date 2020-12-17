use structopt::StructOpt;

use crate::{ClusterChecker, CheckStatuses, CheckStatus};
use crate::cli::ClusterCliError;
use crate::check::{CheckFailed, CheckResults, CheckResult, CheckSuggestion};
use async_channel::Receiver;

const ISSUE_URL: &str = "https://github.com/infinyon/fluvio/issues/new/choose";

#[derive(Debug, StructOpt)]
pub struct CheckOpt {}

impl CheckOpt {
    pub async fn process(self) -> Result<(), ClusterCliError> {
        use colored::*;
        println!("{}", "Running pre-startup checks...".bold());
        let mut progress = ClusterChecker::empty()
            .with_preflight_checks()
            .run_with_progress();

        let results = render_check_progress(&mut progress).await;
        render_results_next_steps(&results);

        Ok(())
    }
}

/// Renders individual checks as they occur over time
async fn render_check_progress(progress: &mut Receiver<CheckResult>) -> CheckResults {
    let mut check_results = vec![];
    while let Ok(check_result) = progress.recv().await {
        render_check_result(&check_result);
        check_results.push(check_result);
    }
    check_results
}

/// Renders a slice of `CheckResults` all at once
pub fn render_check_results<R: AsRef<[CheckResult]>>(check_results: R) {
    let check_results = check_results.as_ref();
    for check_result in check_results {
        render_check_result(check_result);
    }
}

/// Render a single check result
pub fn render_check_result(check_result: &CheckResult) {
    use colored::*;

    match check_result {
        Ok(status) => render_check_status(status),
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

/// Render a slice of `CheckStatus`es all at once
pub fn render_check_statuses<R: AsRef<[CheckStatus]>>(check_statuses: R) {
    let check_statuses = check_statuses.as_ref();
    for check_status in check_statuses {
        render_check_status(check_status);
    }
}

/// Render a single check status
pub fn render_check_status(check_status: &CheckStatus) {
    use colored::*;
    use crate::CheckStatus::*;

    match check_status {
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

    if let Some(suggestion) = check_status.suggestion() {
        println!("  💡 {} {}", "suggestion:".bold().cyan(), suggestion,)
    }
}

/// Render a conclusion message based on the number of failures and warnings
pub fn render_next_steps(failures: u32, warnings: u32, installed: bool) {
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

/// Renders a conclusion message based on the number of failures and warnings
pub fn render_statuses_next_steps(check_statuses: &CheckStatuses) {
    use crate::CheckStatus::*;

    let mut installed = false;
    let mut failures = 0;
    let mut warnings = 0;

    for status in check_statuses.iter() {
        match status {
            Pass(_) => (),
            Fail(CheckFailed::AlreadyInstalled) => installed = true,
            Fail(CheckFailed::AutoRecoverable(_)) => warnings += 1,
            Fail(CheckFailed::Unrecoverable(_)) => failures += 1,
        }
    }

    render_next_steps(failures, warnings, installed);
}

/// Prints suggestions of next steps to the user.
///
/// This is useful when the user ran `fluvio cluster check` and needs
/// to do more to start up the cluster. It is not so useful when this
/// `ClusterResults` was produced by already performing the startup.
pub fn render_results_next_steps(check_results: &CheckResults) {
    use colored::*;
    use crate::CheckStatus::*;

    let mut errors = 0;
    let mut failures = 0;
    let mut warnings = 0;
    let mut installed = false;

    for result in check_results.iter() {
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
    render_next_steps(failures, warnings, installed);
}
