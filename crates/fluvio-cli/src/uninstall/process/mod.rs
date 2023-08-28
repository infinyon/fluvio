mod tasks;

use anyhow::Result;
use async_trait::async_trait;
use semver::Version;

use self::tasks::cluster::ClusterUninstallTask;

/// An uninstall process task
#[async_trait]
pub(crate) trait Task {
  /// This Task's title
  fn title(&self) -> &'static str;
  /// Checks wether a `Task` is done
  async fn is_done(&self, process: &UninstallProcess) -> Result<bool>;
  /// Runs a `Task`
  async fn run(&self, process: &UninstallProcess) -> Result<()>;
}

/// State for a Fluvio uninstallation process
pub(crate) struct UninstallProcess {
  version: Version,
}

impl UninstallProcess {
  pub fn new(version: Version) -> Self {
    Self {
      version: version,
    }
  }

  pub async fn start(self) -> Result<()> {
    let steps: Vec<Box<dyn Task>> = vec![
      Box::new(ClusterUninstallTask::new())
      ];

    // In the future based on the Fluvio version we may skip some steps
    let total_steps = steps.len();

    println!("👷🏻‍♀️ Beginning Fluvio uninstall process...");

    for (idx, task) in steps.into_iter().enumerate() {
      println!("👷🏻‍♀️ Checking [{}/{}]: {}", idx + 1, total_steps, task.title());

      if task.is_done(&self).await? {
        println!("👷🏻‍♀️ Uninstall Step already performed, skipping... [{}/{}]: {}", idx + 1, total_steps, task.title());
        continue;
      }

      println!("👷🏻‍♀️ Performing uninstall step [{}/{}]: {}", idx + 1, total_steps, task.title());
      task.run(&self).await?;
    }

    Ok(())
  }
}
