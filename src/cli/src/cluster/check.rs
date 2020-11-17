use structopt::StructOpt;
use fluvio_cluster::ClusterChecker;
use crate::Result;

#[derive(Debug, StructOpt)]
pub struct CheckOpt {
    /// run pre-install checks
    #[structopt(long)]
    pre_install: bool,
}

impl CheckOpt {
    pub async fn process(self) -> Result<()> {
        if self.pre_install {
            ClusterChecker::run_preflight_checks().await?;
        }
        Ok(())
    }
}
