mod process;

use anyhow::Result;
use clap::Parser;
use semver::Version;

use fluvio_cluster::cli::ClusterCmd;

use self::process::UninstallProcess;

#[derive(Parser, Debug)]
pub struct UninstallOpt;

impl UninstallOpt {
    pub async fn process(self) -> Result<()> {
        let version = semver::Version::parse(crate::VERSION).unwrap();
        let uninstall_process = UninstallProcess::new(version);

        uninstall_process.start().await
    }
}
