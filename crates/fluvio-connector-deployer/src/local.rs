use std::process::{Command, Stdio};

use anyhow::{Result, anyhow};
use tempfile::NamedTempFile;
use crate::Deployment;

pub(crate) fn deploy_local(deployment: &Deployment) -> Result<()> {
    let (_, config_path) = NamedTempFile::new()?.keep()?;
    deployment.config.write_to_file(&config_path)?;

    let mut log_path = std::env::current_dir()?;
    log_path.push(&deployment.config.name);
    log_path.set_extension("log");
    let log_file = std::fs::File::create(log_path.as_path())?;

    let mut cmd = Command::new(&deployment.executable);
    cmd.stdin(Stdio::null());
    cmd.stdout(log_file.try_clone()?);
    cmd.stderr(log_file);
    cmd.arg("--config");
    cmd.arg(
        config_path
            .to_str()
            .ok_or_else(|| anyhow!("illegal path of temp config file"))?,
    );
    let child = cmd.spawn()?;
    println!(
        "Connector runs with process id: {}. Log file: {}",
        child.id(),
        log_path.to_string_lossy()
    );
    Ok(())
}
