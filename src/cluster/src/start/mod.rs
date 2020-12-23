use std::path::PathBuf;

pub mod k8;
pub mod local;

const DEFAULT_CHART_REMOTE: &str = "https://charts.fluvio.io";

/// Distinguishes between a Local and Remote helm chart
#[derive(Debug, Clone)]
pub enum ChartLocation {
    /// Local charts must be located at a valid filesystem path.
    Local(PathBuf),
    /// Remote charts will be located at a URL such as `https://...`
    Remote(String),
}

// /// Runs all of the given checks and attempts to fix any errors
// ///
// /// This requires a fixing-function to be given, which takes a
// /// `RecoverableCheck` and returns a `Result<(), UnrecoverableCheck>`.
// ///
// /// If the fixing function was able to fix the problem, it returns
// /// `Ok(())`. Otherwise, it may wrap the failed check and return it,
// /// such as:
// ///
// /// ```ignore
// /// # use fluvio_cluster::{RecoverableCheck, UnrecoverableCheck};
// /// async fn fix(check: RecoverableCheck) -> Result<(), UnrecoverableCheck> {
// ///     // Try to fix the check...
// ///     // If the fix did not succeed:
// ///     Err(UnrecoverableCheck::FailedRecovery(check))
// /// }
// ///
// /// # async fn do_check_and_fix() {
// /// let check_results = check_and_fix(&[todo!("Add some checks")], fix).await;
// /// # }
// /// ```
// pub(crate) async fn check_and_fix<F, R>(checks: &[Box<dyn InstallCheck>], fix: F) -> CheckResults
// where
//     F: Fn(RecoverableCheck) -> R,
//     R: Future<Output = Result<(), UnrecoverableCheck>>,
// {
//     // We want to collect all of the results of the checks
//     let mut results: Vec<CheckResult> = vec![];
//
//     for check in checks {
//         // Perform one individual check
//         let check_result = check.perform_check().await;
//         match check_result {
//             // If the check passed, add it to the results list
//             it @ Ok(CheckStatus::Pass(_)) => results.push(it),
//             // If the check failed but is potentially auto-recoverable, try to recover it
//             Ok(CheckStatus::Fail(CheckFailed::AutoRecoverable(it))) => {
//                 let err = format!("{}", it);
//                 let fix_result = fix(it).await;
//                 match fix_result {
//                     // If the fix worked, return a passed check
//                     Ok(_) => results.push(Ok(CheckStatus::pass(format!("Fixed: {}", err)))),
//                     Err(e) => {
//                         // If the fix failed, wrap the original failed check in Unrecoverable
//                         results.push(Ok(CheckStatus::fail(CheckFailed::Unrecoverable(e))));
//                         // We return upon the first check failure
//                         return CheckResults::from(results);
//                     }
//                 }
//             }
//             it @ Ok(CheckStatus::Fail(_)) => {
//                 results.push(it);
//                 return CheckResults::from(results);
//             }
//             it @ Err(_) => {
//                 results.push(it);
//                 return CheckResults::from(results);
//             }
//         }
//     }
//
//     CheckResults::from(results)
// }
