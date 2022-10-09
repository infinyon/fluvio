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
// use hubutil::PackageMeta;

use crate::Result;
use crate::client::cmd::ClientCmd;
use crate::CliError;

// manifest names
const SMARTMODULE_TOML_FNAME: &str = "Smartmodule.toml";

/// Delete an existing SmartModule with the given name
#[derive(Debug, Parser)]
pub struct DownloadSmartModuleOpt {
    /// SmartModule name: e.g. infinyon/jolt@v0.0.1
    #[clap(value_name = "name", required = true)]
    pkgname: String,

    #[clap(flatten)]
    target: ClusterTarget,

    /// just download package to local filesystem
    #[clap(long)]
    local: bool,
}

#[async_trait]
impl ClientCmd for DownloadSmartModuleOpt {
    async fn process_client<O: Terminal + Debug + Send + Sync>(
        self,
        _out: Arc<O>,
        _fluvio: &Fluvio,
    ) -> Result<()> {
        let pkgfile = download_local(&self.pkgname).await?;
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
async fn download_local(pkgname: &str) -> Result<String> {
    let fname = hubutil::cli_pkgname_to_filename(pkgname)
        .map_err(|_| CliError::HubError(format!("error writing {pkgname}")))?;

    let access = hubutil::HubAccess::default_load().await.map_err(|_| {
        CliError::HubError("missing access credentials, try 'fluvio cloud login'".into())
    })?;
    let url = hubutil::cli_pkgname_to_url(pkgname, &access.remote)
        .map_err(|_| CliError::HubError(format!("invalid pkgname {pkgname}")))?;
    println!("downloading {pkgname} to {fname}");

    let data = hubutil::get_package(&url, &access)
        .await
        .map_err(|_| CliError::HubError(format!("error downloading {pkgname}")))?;

    std::fs::write(&fname, &data)?;
    println!("... downloading complete");
    Ok(fname)
}

// download smartmodule from pkg to cluster
async fn download_cluster(config: FluvioConfig, pkgfile: &str) -> Result<()> {
    println!("trying connection to fluvio {}", config.endpoint);
    let fluvio = Fluvio::connect_with_config(&config).await?;

    let admin = fluvio.admin().await;

    let pm = hubutil::package_get_meta(pkgfile)
        .map_err(|_| CliError::PackageError(format!("error accessing {pkgfile}")))?;
    let vman = &pm.manifest;
    // check for file contents
    let sm_meta_file = vman
        .iter()
        .find(|&e| e == SMARTMODULE_TOML_FNAME)
        .ok_or_else(|| CliError::PackageError("package missing {SMARTMODULE_TOML_FNAME}".into()))?;
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
        .map_err(|_| CliError::PackageError("package missing {SMARTMODULE_TOML_FNAME}".into()))?;
    let sm_meta = SmartModuleMetadata::from_bytes(&sm_meta_bytes)?;
    let sm_wasm_bytes = hubutil::package_get_manifest_file(pkgfile, sm_wasm_file)
        .map_err(|_| CliError::PackageError("package missing {sm_wasm_file}".into()))?;
    let sm_wasm = SmartModuleWasm::from_raw_wasm_bytes(&sm_wasm_bytes)?;

    let sm_id = sm_meta.store_id();
    let spec = SmartModuleSpec {
        meta: Some(sm_meta),
        wasm: sm_wasm,
    };
    admin.create(sm_id, false, spec).await?;
    Ok(())
}
