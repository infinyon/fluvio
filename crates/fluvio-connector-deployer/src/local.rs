use std::{
    process::{Command, Stdio},
    path::Path,
};

use anyhow::{Result, anyhow};
use tracing::debug;
use crate::Deployment;

pub(crate) fn deploy_local<P: AsRef<Path>>(
    deployment: &Deployment,
    output_file: Option<P>,
) -> Result<()> {
    let (stdout, stderr, wait) = if let Some(log_path) = output_file {
        println!("Log file: {}", log_path.as_ref().to_string_lossy());
        let log_file = std::fs::File::create(log_path)?;
        (log_file.try_clone()?.into(), log_file.into(), false)
    } else {
        (Stdio::inherit(), Stdio::inherit(), true)
    };

    debug!(
        "running executable: {}",
        &deployment.executable.to_string_lossy()
    );
    let mut cmd = Command::new(&deployment.executable);
    cmd.stdin(Stdio::null());
    cmd.stdout(stdout);
    cmd.stderr(stderr);
    cmd.arg("--config");
    cmd.arg(
        deployment
            .config
            .to_str()
            .ok_or_else(|| anyhow!("illegal path of temp config file"))?,
    );
    let mut child = cmd.spawn()?;
    println!("Connector runs with process id: {}", child.id());
    if wait {
        child.wait()?;
    }
    Ok(())
}
