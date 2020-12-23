use structopt::StructOpt;

use crate::ClusterChecker;
use crate::cli::ClusterCliError;
use crate::check::render::{render_check_progress, render_results_next_steps};

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
