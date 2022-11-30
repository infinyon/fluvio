use std::sync::Arc;
use std::fmt::Debug;
use std::path::Path;

use async_trait::async_trait;
use clap::Parser;

use fluvio::Fluvio;
use fluvio::FluvioConfig;
use fluvio::metadata::smartmodule::SmartModuleSpec;
use fluvio_controlplane_metadata::smartmodule::{SmartModuleMetadata, SmartModuleWasm};
use fluvio_extension_common::Terminal;
use fluvio_extension_common::target::ClusterTarget;
use fluvio_hub_util as hubutil;
use hubutil::HubAccess;
use tracing::info;

use crate::{CliError, Result};
use crate::client::cmd::ClientCmd;
use crate::client::hub::get_hub_access;

/// Download a SmartModule from the hub
#[derive(Debug, Parser)]
pub struct DownloadHubOpt {
    /// SmartModule name: e.g. infinyon/jolt@v0.0.1
    #[clap(value_name = "name", required = true)]
    pkgname: String,

    #[clap(flatten)]
    target: ClusterTarget,

    /// just download package to local filesystem
    #[clap(long)]
    local: bool,

    /// given local package file, download to cluster
    #[clap(long)]
    ipkg: bool,

    #[clap(long, hide_short_help = true)]
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
            // pkgname is a package file
            let fluvio_config = self.target.load()?;
            download_cluster(fluvio_config, &self.pkgname).await?;
            return Ok(());
        }
        let access = get_hub_access(&self.remote)?;

        let pkgfile = download_local(&self.pkgname, &access).await?;
        if self.local {
            return Ok(());
        }

        let fluvio_config = self.target.load()?;
        download_cluster(fluvio_config, &pkgfile).await?;
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

    std::fs::write(&fname, &data)?;
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
