use async_trait::async_trait;
use anyhow::Result;

use fluvio_cluster::cli::{ClusterCmd, CheckOpt};

use crate::uninstall::process::{Task, UninstallProcess};

pub struct ClusterUninstallTask;

impl ClusterUninstallTask {
    pub fn new() -> Self {
        Self {}
    }
}

#[async_trait]
impl Task for ClusterUninstallTask {
    fn title(&self) -> &'static str {
        "Remove Fluvio Clusters"
    }

    async fn is_done(&self, process: &UninstallProcess) -> Result<bool> {
        let cluster_check_cmd = CheckOpt { fix: false };

        cluster_check_cmd.process(process.version.clone()).await?;

        Ok(true)
    }

    async fn run(&self, process: &UninstallProcess) -> Result<()> {
        println!("Running cluster uninstall task");
        Ok(())
    }
}
