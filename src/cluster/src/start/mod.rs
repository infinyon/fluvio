use std::future::Future;
pub mod k8;
pub mod local;
use crate::check::{InstallCheck, CheckFailed, RecoverableCheck, CheckResults, CheckResult};
use crate::{UnrecoverableCheck, CheckStatus};

/// Runs all of the given checks and attempts to fix any errors
///
/// This requires a fixing-function to be given, which takes a
/// `RecoverableCheck` and returns a `Result<(), UnrecoverableCheck>`.
///
/// If the fixing function was able to fix the problem, it returns
/// `Ok(())`. Otherwise, it may wrap the failed check and return it,
/// such as:
///
/// ```no_run
/// # use fluvio_cluster::UnrecoverableCheck;
/// async fn fix(check: RecoverableCheck) -> Result<(), UnrecoverableCheck> {
///     // Try to fix the check...
///     // If the fix did not succeed:
///     Err(UnrecoverableCheck::FailedRecovery(check))
/// }
///
/// # async fn do_check_and_fix() {
/// let check_results = check_and_fix(&[todo!("Add some checks")], fix).await;
/// # }
/// ```
pub(crate) async fn check_and_fix<F, R>(checks: &[Box<dyn InstallCheck>], fix: F) -> CheckResults
where
    F: Fn(RecoverableCheck) -> R,
    R: Future<Output = Result<(), UnrecoverableCheck>>,
{
    // We want to collect all of the results of the checks
    let mut results: Vec<CheckResult> = vec![];

    for check in checks {
        // Perform one individual check
        let check_result = check.perform_check().await;
        match check_result {
            // If the check failed but is potentially auto-recoverable, try to recover it
            Ok(CheckStatus::Fail(CheckFailed::AutoRecoverable(it))) => {
                let err = format!("{}", it);
                let fix_result = fix(it).await;
                match fix_result {
                    // If the fix worked, return a passed check
                    Ok(_) => results.push(Ok(CheckStatus::pass(format!("Fixed: {}", err)))),
                    // If the fix failed, wrap the original failed check in Unrecoverable
                    Err(e) => results.push(Ok(CheckStatus::fail(CheckFailed::Unrecoverable(e)))),
                }
            }
            // If the check passed, errored, or otherwise failed, just collect it
            other => results.push(other),
        }
    }

    CheckResults::from(results)
}
