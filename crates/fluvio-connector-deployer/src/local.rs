use std::fs::canonicalize;
use std::path::Path;
use std::process::{Command, Stdio};

use anyhow::{Context, Result};
use clap::ValueEnum;
use enum_display::EnumDisplay;
use tracing::debug;

use crate::Deployment;

#[derive(ValueEnum, Debug, Clone, PartialEq, Eq, Default, EnumDisplay)]
#[clap(rename_all = "kebab-case")]
#[enum_display(case = "Kebab")]
pub enum LogLevel {
    Trace,
    Debug,
    #[default]
    Info,
    Warn,
    Error,
}

pub(crate) fn deploy_local<P: AsRef<Path>>(
    deployment: &Deployment,
    output_file: Option<P>,
    name: &str,
) -> Result<u32> {
    let (stdout, stderr, wait) = if let Some(log_path) = output_file {
        println!("Log file: {}", log_path.as_ref().to_string_lossy());
        let log_file = std::fs::File::create(log_path)?;
        (log_file.try_clone()?.into(), log_file.into(), false)
    } else {
        (Stdio::inherit(), Stdio::inherit(), true)
    };

    let executable = canonicalize(&deployment.executable).context(format!(
        "Executable file path ({}) is invalid or file does not exist",
        deployment.executable.to_string_lossy()
    ))?;
    debug!("running executable: {}", &executable.to_string_lossy());
    let mut cmd = Command::new(executable);

    cmd.env("RUST_LOG", deployment.log_level.to_string());
    cmd.stdin(Stdio::null());
    cmd.stdout(stdout);
    cmd.stderr(stderr);
    cmd.arg("--config");
    cmd.arg(
        canonicalize(&deployment.config)
            .context("Config file path is invalid or file does not exist")?,
    );
    if let Some(secrets) = &deployment.secrets {
        cmd.arg("--secrets");
        cmd.arg(
            canonicalize(secrets).context("Secrets file path is invalid or file does not exist")?,
        );
    }
    let mut child = cmd.spawn()?;
    println!("Connector runs with process id: {}", child.id());
    println!("Started connector `{}`", name);
    if wait {
        child.wait()?;
    }
    Ok(child.id())
}
