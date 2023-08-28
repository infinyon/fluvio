use async_trait::async_trait;
use anyhow::Result;

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

  /// Checks wether a `Task` is done
  async fn is_done(&self, process: &UninstallProcess) -> Result<bool> {
    Ok(false)
  }

  /// Runs a `Task`
  async fn run(&self, process: &UninstallProcess) -> Result<()> {
    println!("Running cluster uninstall task");
    Ok(())
  }
}
