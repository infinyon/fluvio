use std::{
    fmt::Debug,
    path::{PathBuf, Path},
    ffi::OsStr,
    fs::{File, Permissions},
    io::Write,
};

use anyhow::{Result, Context, anyhow};
use clap::{Parser, Subcommand};

use cargo_builder::package::PackageInfo;
use fluvio_connector_deployer::{Deployment, DeploymentType};
use fluvio_connector_package::metadata::ConnectorMetadata;
use tracing::{debug, trace};

use crate::cmd::PackageCmd;

const CONNECTOR_METADATA_FILE_NAME: &str = "Connector.toml";

/// Deploys the Connector from the current working directory
#[derive(Debug, Parser)]
pub struct DeployCmd {
    #[clap(flatten)]
    package: PackageCmd,

    #[command(subcommand)]
    deployment_type: DeploymentTypeCmd,

    /// Extra arguments to be passed to cargo
    #[clap(raw = true)]
    extra_arguments: Vec<String>,
}

#[derive(Debug, Subcommand)]
enum DeploymentTypeCmd {
    Local {
        #[clap(short, long, value_name = "PATH")]
        config: PathBuf,

        /// Deploy from local package file
        #[clap(long = "ipkg", value_name = "PATH")]
        ipkg_file: Option<PathBuf>,
    },
}

impl DeployCmd {
    pub(crate) fn process(self) -> Result<()> {
        let DeployCmd {
            package,
            deployment_type,
            extra_arguments: _,
        } = self;
        match deployment_type {
            DeploymentTypeCmd::Local { config, ipkg_file } => {
                deploy_local(package, config, ipkg_file)
            }
        }
    }
}

fn deploy_local(
    package_cmd: PackageCmd,
    config: PathBuf,
    ipkg_file: Option<PathBuf>,
) -> Result<()> {
    let (executable, connector_metadata) = match ipkg_file {
        Some(ipkg_file) => from_ipkg_file(ipkg_file).context("Failed to deploy from ipkg file")?,
        None => from_cargo_package(package_cmd)
            .context("Failed to deploy from within cargo package directory")?,
    };

    let mut log_path = std::env::current_dir()?;
    log_path.push(&connector_metadata.package.name);
    log_path.set_extension("log");

    let mut builder = Deployment::builder();
    builder
        .executable(executable)
        .config(config)
        .pkg(connector_metadata)
        .deployment_type(DeploymentType::Local {
            output_file: Some(log_path),
        });
    builder.deploy()
}

pub(crate) fn from_cargo_package(package_cmd: PackageCmd) -> Result<(PathBuf, ConnectorMetadata)> {
    debug!("reading connector metadata from cargo package");
    let opt = package_cmd.as_opt();
    let p = PackageInfo::from_options(&opt)?;
    let connector_metadata =
        ConnectorMetadata::from_toml_file(p.package_relative_path(CONNECTOR_METADATA_FILE_NAME))?;
    let executable_path = p.target_bin_path()?;
    Ok((executable_path, connector_metadata))
}

fn from_ipkg_file(ipkg_file: PathBuf) -> Result<(PathBuf, ConnectorMetadata)> {
    println!("... checking package");
    debug!(
        "reading connector metadata from ipkg file {}",
        ipkg_file.to_string_lossy()
    );
    let package_meta = fluvio_hub_util::package_get_meta(ipkg_file.to_string_lossy().as_ref())
        .context("Failed to read package metadata")?;
    let entries: Vec<&Path> = package_meta.manifest.iter().map(Path::new).collect();

    let connector_toml = entries
        .iter()
        .find(|e| {
            e.file_name()
                .eq(&Some(OsStr::new(CONNECTOR_METADATA_FILE_NAME)))
        })
        .ok_or_else(|| anyhow!("Package missing {} file", CONNECTOR_METADATA_FILE_NAME))?;
    let connector_toml_bytes =
        fluvio_hub_util::package_get_manifest_file(&ipkg_file, connector_toml)?;
    let connector_metadata = ConnectorMetadata::from_toml_slice(&connector_toml_bytes)?;
    trace!("{:#?}", connector_metadata);

    let binary_name = connector_metadata
        .deployment
        .binary
        .as_ref()
        .ok_or_else(|| anyhow!("Only binary deployments are supported at this moment"))?;
    let binary = entries
        .iter()
        .find(|e| e.file_name().eq(&Some(OsStr::new(&binary_name))))
        .ok_or_else(|| anyhow!("Package missing {} file", binary_name))?;

    let binary_bytes = fluvio_hub_util::package_get_manifest_file(&ipkg_file, binary)?;
    let mut executable_path = ipkg_file;
    executable_path.pop();
    executable_path.push(binary_name);
    let mut file = File::create(&executable_path)?;
    set_exec_permissions(&mut file)?;
    file.write_all(&binary_bytes)?;

    Ok((executable_path, connector_metadata))
}

#[cfg(unix)]
fn set_exec_permissions(f: &mut File) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;

    f.set_permissions(Permissions::from_mode(0o744))?;
    Ok(())
}

#[cfg(not(unix))]
fn set_exec_permissions(f: &mut File) -> Result<()> {
    Ok(())
}
