#![allow(unused)]

use futures_util::StreamExt;
use futures_channel::mpsc::Receiver;
use crate::{
    CheckFailed, CheckResult, CheckResults, CheckStatus, CheckSuggestion,
    render::ProgressRenderedText,
};

const ISSUE_URL: &str = "https://github.com/infinyon/fluvio/issues/new/choose";

/// Renders individual checks as they occur over time
pub async fn render_check_progress(progress: &mut Receiver<CheckResult>) -> CheckResults {
    let mut check_results = vec![];
    while let Some(check_result) = progress.next().await {
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
    println!("{}", check_result.text());
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
    println!("{}", check_status.text());
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
pub fn render_statuses_next_steps<R: AsRef<[CheckStatus]>>(check_statuses: R) {
    use crate::CheckStatus::*;

    let mut installed = false;
    let mut failures = 0;
    let mut warnings = 0;

    for status in check_statuses.as_ref().iter() {
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
pub fn render_results_next_steps<R: AsRef<[CheckResult]>>(check_results: R) {
    use colored::*;
    use crate::CheckStatus::*;

    let mut errors = 0;
    let mut failures = 0;
    let mut warnings = 0;
    let mut installed = false;

    for result in check_results.as_ref().iter() {
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
        println!("If you believe this is a bug, please consider filing a report ❤️");
        println!("  {}", ISSUE_URL.bold());
        return;
    }

    // Print a conclusion message based on warnings and failures
    render_next_steps(failures, warnings, installed);
}

impl ProgressRenderedText for CheckStatus {
    fn text(&self) -> String {
        use colored::*;
        use crate::CheckStatus::*;

        let mut text = match self {
            Pass(success) => {
                format!("{:>13} {}", "Ok: ✅".bold().green(), success)
            }
            Fail(e @ CheckFailed::AutoRecoverable(_)) => {
                format!(
                    "{:>11} {}\n{:indent$}💡 {}",
                    "Warn: 🟡️".bold().yellow(),
                    e,
                    "",
                    format!(
                        "{:>11} {}",
                        "note:".bold(),
                        "This may be fixed automatically during startup"
                    ),
                    indent = 9,
                )
            }
            Fail(CheckFailed::AlreadyInstalled) => {
                format!(
                    "💙 {} {}",
                    "note:".bold().bright_blue(),
                    "Fluvio is already running".bright_blue()
                )
            }
            Fail(e @ CheckFailed::Unrecoverable(_)) => {
                // Print one layer of source error
                let cause = match std::error::Error::source(e) {
                    Some(underlying) => format!(": {}", underlying),
                    None => "".to_string(),
                };
                let msg = format!("{}{}", e, cause);
                format!("❌ {} {}", "failed:".bold().red(), msg.red())
            }
        };

        if let Some(suggestion) = self.suggestion() {
            text.push_str(&format!(
                "\n{:indent$}💡 {:>11} {}",
                "",
                "suggestion:".bold().cyan(),
                suggestion,
                indent = 9,
            ));
        }
        text
    }
}

impl ProgressRenderedText for CheckResult {
    fn text(&self) -> String {
        use colored::*;

        match self {
            Ok(status) => status.text(),
            Err(e) => {
                // Print one layer of source error
                let cause = match std::error::Error::source(e) {
                    Some(underlying) => format!(": {}", underlying),
                    None => "".to_string(),
                };
                let msg = format!("{}{}", e, cause);
                format!("🚨 {} {}", "[ERROR]:".bold().red(), msg.red())
            }
        }
    }
}
