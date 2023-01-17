use clap::Parser;
use tracing::debug;
use anyhow::Result;

use fluvio_index::{PackageId, HttpAgent, MaybeVersion};

use super::update::should_always_print_available_update;

use fluvio_cli_common::error::CliError as CommonCliError;
use fluvio_cli_common::error::HttpError;
use fluvio_cli_common::install::{
    fetch_latest_version, fetch_package_file, fluvio_extensions_dir, install_bin, install_println,
    fluvio_bin_dir,
};
use crate::error::CliError;
use crate::install::update::{
    check_update_required, prompt_required_update, check_update_available, prompt_available_update,
};

#[derive(Parser, Debug)]
pub struct InstallOpt {
    /// The ID of a package to install, e.g. "fluvio/fluvio-cloud".
    package: Option<PackageId<MaybeVersion>>,
    /// Used for testing. Specifies alternate package location, e.g. "test/"
    #[clap(hide = true, long)]
    prefix: Option<String>,
    /// Install the latest prerelease rather than the latest release
    ///
    /// If the package ID contains a version (e.g. `fluvio/fluvio:0.6.0`), this is ignored
    #[clap(long)]
    pub develop: bool,

    /// When this flag is provided, use the hub. Dev-only
    #[clap(long)]
    pub hub: bool,
}

// Copied from hub-tool (rough hacks to error handling to make this work)
use fluvio_hub_util as hubutil;
use hubutil::http;
use hubutil::http::StatusCode;
use hubutil::HubAccess;
pub const HUB_API_BPKG_AUTH: &str = "hub/v0/bpkg-auth"; // copied from hub-tool
async fn get_binary(
    channel: &str,
    systuple: &str,
    binname: &str,
    access: &HubAccess,
) -> Result<Vec<u8>> {
    let actiontoken = access.get_download_token().await.map_err(|_| {
        CommonCliError::HttpError(HttpError::InvalidInput("authorization error".into()))
    })?;

    let binurl = format!(
        "{}/{HUB_API_BPKG_AUTH}/{channel}/{systuple}/{binname}",
        access.remote
    );
    debug!("accessing url: {binurl}");
    let mut resp = http::get(binurl)
        .header("Authorization", actiontoken)
        .await
        .map_err(|_| {
            CommonCliError::HttpError(HttpError::InvalidInput("authorization error".into()))
        })?;

    match resp.status() {
        StatusCode::Ok => {}
        code => {
            let body_err_message = resp
                .body_string()
                .await
                .unwrap_or_else(|_err| "couldn't fetch error message".to_string());
            let msg = format!("Status({code}) {body_err_message}");
            return Err(crate::CliError::HubError(msg).into());
            //return Err(HubCliError::Cmd(msg));
            //return Err(CommonCliError::HttpError(HttpError::InvalidInput("authorization error".into())));
        }
    }
    let data = resp
        .body_bytes()
        .await
        .map_err(|_| crate::CliError::HubError("Data unpack failure".into()))?;
    Ok(data)
}

impl InstallOpt {
    pub async fn process(self) -> Result<()> {
        if self.hub {
            debug!("Using the hub to install");

            let channel = "latest";
            let systuple = "aarch64-apple-darwin";
            let binname = "sample-bin-script";

            let access =
                HubAccess::default_load(&Some("https://hub-dev.infinyon.cloud".to_string()))
                    .map_err(|_| {
                        crate::CliError::Other(
                            "Something happened getting hub dev info".to_string(),
                        )
                    })?;
            let data = get_binary(channel, systuple, binname, &access).await?;
            std::fs::write(binname, data)?;
        } else {
            let agent = match &self.prefix {
                Some(prefix) => HttpAgent::with_prefix(prefix)?,
                None => HttpAgent::default(),
            };

            // Before any "install" type command, check if the CLI needs updating.
            // This may be the case if the index schema has updated.
            let require_update = check_update_required(&agent).await?;
            if require_update {
                prompt_required_update(&agent).await?;
                return Ok(());
            }

            let result = self.install_plugin(&agent).await;
            match result {
                Ok(_) => (),
                Err(err) => match err.downcast_ref::<CliError>() {
                    Some(crate::CliError::IndexError(fluvio_index::Error::MissingTarget(
                        target,
                    ))) => {
                        install_println(format!(
                            "❕ Package '{}' is not available for target {}, skipping",
                            self.package
                                .ok_or(crate::CliError::Other(
                                    "Package name not provided".to_string(),
                                ))?
                                .name(),
                            target
                        ));
                        install_println("❕ Consider filing an issue to add support for this platform using the link below! 👇");
                        install_println(format!(
                    "❕   https://github.com/infinyon/fluvio/issues/new?title=Support+fluvio-cloud+on+target+{}",
                    target
                ));
                        return Ok(());
                    }
                    _ => return Err(err),
                },
            }

            // After any "install" command, check if the CLI has an available update,
            // i.e. one that is not required, but present.
            // Sometimes this is printed at the beginning, so we don't print it again here
            if !should_always_print_available_update() {
                let update_result = check_update_available(&agent, false).await;
                if let Ok(Some(latest_version)) = update_result {
                    prompt_available_update(&latest_version);
                }
            }
        }
        Ok(())
    }

    // Error handling hacked together
    async fn install_plugin(&self, agent: &HttpAgent) -> Result<()> {
        let target = fluvio_index::package_target()?;

        // If a version is given in the package ID, use it. Otherwise, use latest
        let id = match self
            .package
            .clone()
            .ok_or(crate::CliError::Other(
                "Package name not provided".to_string(),
            ))?
            .maybe_version()
        {
            Some(version) => {
                install_println(format!(
                    "⏳ Downloading package with provided version: {}...",
                    &self.package.clone().ok_or(crate::CliError::Other(
                        "Package name not provided".to_string(),
                    ))?
                ));
                let version = version.clone();
                self.package
                    .clone()
                    .ok_or(crate::CliError::Other(
                        "Package name not provided".to_string(),
                    ))?
                    .into_versioned(version)
            }
            None => {
                let id = &self.package.clone().ok_or(crate::CliError::Other(
                    "Package name not provided".to_string(),
                ))?;
                install_println(format!("🎣 Fetching latest version for package: {}...", id));
                let version = fetch_latest_version(agent, id, &target, self.develop).await?;
                let id = id.clone().into_versioned(version.into());
                install_println(format!(
                    "⏳ Downloading package with latest version: {}...",
                    id
                ));
                id
            }
        };

        // Download the package file from the package registry
        let package_result = fetch_package_file(agent, &id, &target).await;
        let package_file = match package_result {
            Ok(pf) => pf,
            Err(CommonCliError::PackageNotFound {
                package,
                version,
                target,
            }) => {
                install_println(format!(
                    "❕ Package {} is not published at {} for {}, skipping",
                    package, version, target
                ));
                return Ok(());
            }
            Err(other) => return Err(other.into()),
        };
        install_println("🔑 Downloaded and verified package file");

        // Install the package to the ~/.fluvio/bin/ dir
        // If the plugin name doesn't start with `fluvio-`, then install it to the bin dir
        let fluvio_dir = if id.name().to_string().starts_with("fluvio-") {
            fluvio_extensions_dir()?
        } else {
            fluvio_bin_dir()?
        };
        debug!("{fluvio_dir:#?}");

        let package_filename = if target.to_string().contains("windows") {
            format!("{}.exe", id.name().as_str())
        } else {
            id.name().to_string()
        };
        let package_path = fluvio_dir.join(package_filename);
        install_bin(package_path, package_file)?;

        Ok(())
    }
}
