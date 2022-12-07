use std::process::Command;

use anyhow::{Result, anyhow};
use tempfile::NamedTempFile;
use crate::Deployment;

pub(crate) fn deploy_local(deployment: &Deployment) -> Result<()> {
    let (_, config_path) = NamedTempFile::new()?.keep()?;
    deployment.config.write_to_file(&config_path)?;

    let mut cmd = Command::new(&deployment.executable);
    cmd.arg("--config");
    cmd.arg(
        config_path
            .to_str()
            .ok_or_else(|| anyhow!("illegal path of temp config file"))?,
    );
    cmd.spawn()?;
    Ok(())
}
