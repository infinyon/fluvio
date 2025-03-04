use std::sync::Arc;
use std::fmt::Debug;
use std::path::Path;
use std::path::PathBuf;

use async_trait::async_trait;
use clap::Parser;
use fluvio::FluvioAdmin;
use tracing::info;
use anyhow::Result;

use fluvio::Fluvio;
use fluvio::metadata::smartmodule::SmartModuleSpec;
use fluvio_controlplane_metadata::smartmodule::{SmartModuleMetadata, SmartModuleWasm};
use fluvio_extension_common::Terminal;
use fluvio_extension_common::target::ClusterTarget;
use fluvio_hub_util as hubutil;
use hubutil::HubAccess;
use hubutil::cmd::get_hub_access;

use crate::CliError;
use crate::client::cmd::ClientCmd;

/// Download a SmartModule from the hub
#[derive(Debug, Parser)]
pub struct SmartModuleDownloadHubOpts {
    /// SmartModule name: e.g. infinyon/jolt@v0.0.1
    #[arg(value_name = "name", required = true)]
    pkgname: String,

    #[clap(flatten)]
    target: ClusterTarget,

    /// Download package to local filesystem
    #[arg(short, long, value_name = "PATH")]
    output: Option<PathBuf>,

    /// given local package file, download to cluster
    #[arg(long)]
    ipkg: bool,

    #[arg(long, hide_short_help = true)]
    remote: Option<String>,
}

#[async_trait]
impl ClientCmd for SmartModuleDownloadHubOpts {
    async fn process_client<O: Terminal + Debug + Send + Sync>(
        self,
        _out: Arc<O>,
        _fluvio: &Fluvio,
    ) -> Result<()> {
        let config = self.target.load()?;
        println!("trying connection to fluvio {}", config.endpoint);
        let fluvio = Fluvio::connect_with_config(&config).await?;
        let admin = fluvio.admin().await;
        if self.ipkg {
            // pkgname is a package file
            download_cluster(&admin, &self.pkgname).await?;
            return Ok(());
        }
        let access = get_hub_access(&self.remote)?;

        let pkgfile = download_local(&self.pkgname, &access, self.output.clone()).await?;
        if self.output.is_some() {
            return Ok(());
        }

        download_cluster(&admin, &pkgfile).await?;
        Ok(())
    }
}

/// download smartmodule from hub to local fs
/// returns path of downloaded of package
pub async fn download_local(
    pkgname: &str,
    access: &HubAccess,
    output: Option<PathBuf>,
) -> Result<String> {
    let file_name = hubutil::cli_pkgname_to_filename(pkgname).map_err(|_| {
        CliError::HubError(format!(
            "invalid package name format {pkgname}, is it the form infinyon/json-sql@0.1.0"
        ))
    })?;

    let file_path = if let Some(mut output) = output {
        if output.is_dir() {
            output.push(file_name);
        }
        output
    } else {
        PathBuf::from(file_name)
    };

    let url = hubutil::cli_pkgname_to_url(pkgname, &access.remote)
        .map_err(|_| CliError::HubError(format!("invalid pkgname {pkgname}")))?;
    println!("downloading {pkgname} to {}", file_path.display());

    let data = hubutil::get_package(&url, access)
        .await
        .map_err(|err| CliError::HubError(format!("downloading {pkgname}\nServer: {err}")))?;

    std::fs::write(&file_path, data)?;
    println!("... downloading complete");
    Ok(file_path.display().to_string())
}

// download smartmodule from pkg to cluster
pub async fn download_cluster(admin: &FluvioAdmin, pkgfile: &str) -> Result<()> {
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

    admin.create(sm_id, false, spec).await?;
    println!("... cluster smartmodule install complete");
    std::fs::remove_file(pkgfile)
        .map_err(|_| CliError::PackageError(format!("error deleting temporary pkg {pkgfile}")))?;
    Ok(())
}
