use crate::CliError;
use super::*;
use fluvio_cluster::ClusterChecker;

#[derive(Debug, StructOpt)]
pub struct CheckCommand {
    /// run pre-install checks
    #[structopt(long)]
    pre_install: bool,
}

pub async fn run_checks(opt: CheckCommand) -> Result<String, CliError> {
    if opt.pre_install {
        ClusterChecker::run_preflight_checks().await?;
    }
    Ok("".to_string())
}
