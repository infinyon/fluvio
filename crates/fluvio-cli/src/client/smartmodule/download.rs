use std::sync::Arc;
use std::fmt::Debug;

use async_trait::async_trait;
use clap::Parser;

use fluvio::Fluvio;
use fluvio_extension_common::Terminal;
use fluvio_extension_common::target::ClusterTarget;
use fluvio::FluvioConfig;
use fluvio_future::task::run_block_on;
// use fluvio::metadata::smartmodule::SmartModuleSpec;
use fluvio_hub_util as hubutil;

use crate::Result;
use crate::client::cmd::ClientCmd;
use crate::CliError;

/// Delete an existing SmartModule with the given name
#[derive(Debug, Parser)]
pub struct DownloadSmartModuleOpt {
    /// SmartModule name: e.g. infinyon/jolt@v0.0.1
    #[clap(value_name = "name", required = true)]
    pkgname: String,

    #[clap(flatten)]
    target: ClusterTarget,

    /// download package to local filesystem
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
        if self.local {
            if let Err(e) = run_block_on(download_local(&self.pkgname)) {
                eprintln!("{}", e);
                std::process::exit(1);
            }
            return Ok(());
        }

        let fluvio_config = self.target.clone().load()?;
        if let Err(e) = run_block_on(download_cluster(fluvio_config, &self.pkgname)) {
            eprintln!("{}", e);
            std::process::exit(1);
        }
        Ok(())
    }
}

// download smartmodule from hub to cluster
async fn download_cluster(config: FluvioConfig, pkgname: &str) -> Result<()> {
    println!("trying connectiong to fluvio {}", config.endpoint);
    let fluvio = Fluvio::connect_with_config(&config).await?;

    let _admin = fluvio.admin().await;
    println!("downloading smartmodule: {}", pkgname);
    Ok(())
}

// download smartmodule from hub to local fs
async fn download_local(pkgname: &str) -> Result<()> {
    let access = hubutil::HubAccess::default_load().await.map_err(|_| {
        CliError::HubError("missing access credentials, try 'fluvio cloud login'".into())
    })?;
    let url = hubutil::cli_pkgname_to_url(pkgname, &access.remote)
        .map_err(|_| CliError::HubError(format!("invalid pkgname {pkgname}")))?;
    println!("local download {}", pkgname);

    let data = hubutil::get_package(&url, &access)
        .await
        .map_err(|_| CliError::HubError(format!("error downloading {pkgname}")))?;
    let fname = hubutil::cli_pkgname_to_filename(pkgname)
        .map_err(|_| CliError::HubError(format!("error writing {pkgname}")))?;
    std::fs::write(&fname, &data)?;

    Ok(())
}
