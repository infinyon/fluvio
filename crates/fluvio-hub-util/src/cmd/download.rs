use std::path::PathBuf;
use std::sync::Arc;
use std::fmt::Debug;

use clap::Parser;
use anyhow::{Result, anyhow};

use fluvio_extension_common::Terminal;

use crate::{cli_pkgname_to_filename, cli_conn_pkgname_to_url, get_package};

use super::get_hub_access;

/// Download Connector to the local folder
#[derive(Debug, Parser)]
#[command(arg_required_else_help = true)]
pub struct ConnectorHubDownloadOpts {
    /// Connector name: e.g. infinyon/http-sink@vX.Y.Z
    #[arg(value_name = "name", required = true)]
    package_name: String,

    /// Target local folder or file name
    #[arg(short, long, value_name = "PATH")]
    output: Option<PathBuf>,

    /// Target platform for the package. Optional. By default the host's one is used.
    #[arg(
        long,
        default_value_t = current_platform::CURRENT_PLATFORM.to_string()
    )]
    target: String,

    #[arg(long, hide_short_help = true)]
    remote: Option<String>,
}

impl ConnectorHubDownloadOpts {
    pub async fn process<O: Terminal + Debug + Send + Sync>(self, _out: Arc<O>) -> Result<()> {
        let access = get_hub_access(&self.remote)?;

        let package_name = self.package_name;
        let file_name = cli_pkgname_to_filename(&package_name).map_err(|_| {
            anyhow!("invalid package name format {package_name}, is it the form infinyon/json-sql@0.1.0")
        })?;

        let file_path = if let Some(mut output) = self.output {
            if output.is_dir() {
                output.push(file_name);
            }
            output
        } else {
            PathBuf::from(file_name)
        };
        let path = file_path.to_string_lossy();
        println!("downloading {package_name} to {path}");

        let url = cli_conn_pkgname_to_url(&package_name, &access.remote, &self.target)
            .map_err(|_| anyhow!("invalid pkgname {package_name}"))?;

        let data = get_package(&url, &access)
            .await
            .map_err(|err| anyhow!("downloading {package_name} failed\nServer: {err}"))?;

        std::fs::write(file_path, data)
            .map_err(|err| anyhow!("unable to write downloaded package to the disk: {err}"))?;
        println!("... downloading complete");
        Ok(())
    }
}
