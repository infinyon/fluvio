use std::sync::Arc;
use std::fmt::Debug;
use std::path::Path;

use anyhow::{Error, Result};
use async_trait::async_trait;
use clap::Parser;
use tracing::info;

use fluvio::Fluvio;
use fluvio::FluvioConfig;
use fluvio::metadata::smartmodule::SmartModuleSpec;
use fluvio_controlplane_metadata::smartmodule::{SmartModuleMetadata, SmartModuleWasm};
use fluvio_extension_common::Terminal;
use fluvio_extension_common::target::ClusterTarget;
use fluvio_hub_util as hubutil;
use hubutil::HubAccess;

use crate::CliError;
use crate::client::cmd::ClientCmd;
use crate::client::hub::get_hub_access;

#[derive(Debug)]
pub enum PackageName {
    Connector(String),
    SmartModule(String),
}

impl TryFrom<DownloadHubOpt> for PackageName {
    type Error = Error;

    fn try_from(opt: DownloadHubOpt) -> Result<Self> {
        if let Some(connector) = opt.connector {
            return Ok(Self::Connector(connector));
        }

        if let Some(smartmodule) = opt.smartmodule {
            return Ok(Self::SmartModule(smartmodule));
        }

        Err(Error::msg("No package type specified"))
    }
}

/// Download a Connector or a SmartModule from the Hub
#[derive(Debug, Parser)]
pub struct DownloadHubOpt {
    /// Specify a Connector to download into Cluster
    #[arg(long = "conn", conflicts_with = &"smartmodule")]
    connector: Option<String>,

    /// Specify a SmartModule to download into Cluster
    #[arg(long = "sm", conflicts_with = &"connector")]
    smartmodule: Option<String>,

    #[clap(flatten)]
    target: ClusterTarget,

    /// Download package to local filesystem
    #[arg(long)]
    local: bool,

    /// given local package file, download to cluster
    #[arg(long)]
    ipkg: bool,

    #[arg(long, hide_short_help = true)]
    remote: Option<String>,
}

#[async_trait]
impl ClientCmd for DownloadHubOpt {
    async fn process_client<O: Terminal + Debug + Send + Sync>(
        self,
        _out: Arc<O>,
        _fluvio: &Fluvio,
    ) -> Result<()> {
        if self.ipkg {
            
        }

        let pkg = PackageName::try_from(self)?;

        Ok(())
    }
}

/// download smartmodule from hub to local fs
/// returns path of downloaded of package
async fn download_local(pkgname: &str, access: &HubAccess) -> Result<String> {
    let fname = hubutil::cli_pkgname_to_filename(pkgname).map_err(|_| {
        CliError::HubError(format!(
            "invalid package name format {pkgname}, is it the form infinyon/json-sql@0.1.0"
        ))
    })?;

    let url = hubutil::cli_pkgname_to_url(pkgname, &access.remote)
        .map_err(|_| CliError::HubError(format!("invalid pkgname {pkgname}")))?;
    println!("downloading {pkgname} to {fname}");

    let data = hubutil::get_package(&url, access)
        .await
        .map_err(|err| CliError::HubError(format!("downloading {pkgname}\nServer: {err}")))?;

    std::fs::write(&fname, data)?;
    println!("... downloading complete");
    Ok(fname)
}

// download smartmodule from pkg to cluster
async fn download_cluster(config: FluvioConfig, pkgfile: &str) -> Result<()> {
    println!("... checking package");
    let pm = hubutil::package_get_meta(pkgfile)
        .map_err(|_| CliError::PackageError(format!("accessing metadata in {pkgfile}")))?;
    let vman = &pm.manifest;
    // check for file contents
    let sm_meta_file = vman
        .iter()
        .find(|&e| {
            let ext = Path::new(e).extension().unwrap_or_default();
            ext == "toml"
        })
        .ok_or_else(|| CliError::PackageError("package missing SmartModule toml".into()))?;
    info!(sm_meta_file, "found SmartModule meta file");
    let sm_wasm_file = vman
        .iter()
        .find(|&e| {
            let epath = Path::new(e);
            let ext = epath.extension().unwrap_or_default();
            ext == "wasm"
        })
        .ok_or_else(|| CliError::PackageError("package missing wasm file".into()))?;

    // extract files
    let sm_meta_bytes = hubutil::package_get_manifest_file(pkgfile, sm_meta_file)
        .map_err(|_| CliError::PackageError("package missing Smartmodule toml".into()))?;
    let sm_meta = SmartModuleMetadata::from_bytes(&sm_meta_bytes)?;
    let sm_wasm_bytes = hubutil::package_get_manifest_file(pkgfile, sm_wasm_file)
        .map_err(|_| CliError::PackageError(format!("package missing {sm_wasm_file}")))?;
    let sm_wasm = SmartModuleWasm::from_raw_wasm_bytes(&sm_wasm_bytes)?;

    let sm_id = sm_meta.store_id();
    let spec = SmartModuleSpec {
        meta: Some(sm_meta),
        wasm: sm_wasm,
        ..Default::default()
    };

    println!("trying connection to fluvio {}", config.endpoint);
    let fluvio = Fluvio::connect_with_config(&config).await?;

    let admin = fluvio.admin().await;
    admin.create(sm_id, false, spec).await?;
    println!("... cluster smartmodule install complete");
    std::fs::remove_file(pkgfile)
        .map_err(|_| CliError::PackageError(format!("error deleting temporary pkg {pkgfile}")))?;
    Ok(())
}
