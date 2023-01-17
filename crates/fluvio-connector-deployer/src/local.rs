use std::process::{Command, Stdio};

use anyhow::{Result, anyhow};
use tracing::debug;
use crate::Deployment;

pub(crate) fn deploy_local(deployment: &Deployment) -> Result<()> {
    let mut log_path = std::env::current_dir()?;
    log_path.push(&deployment.pkg.package.name);
    log_path.set_extension("log");
    let log_file = std::fs::File::create(log_path.as_path())?;

    debug!(
        "running executable: {}",
        &deployment.executable.to_string_lossy()
    );
    let mut cmd = Command::new(&deployment.executable);
    cmd.stdin(Stdio::null());
    cmd.stdout(log_file.try_clone()?);
    cmd.stderr(log_file);
    cmd.arg("--config");
    cmd.arg(
        deployment
            .config
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
