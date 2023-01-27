use clap::Parser;
use tracing::debug;
use anyhow::Result;
use current_platform::CURRENT_PLATFORM;

use fluvio_cli_common::error::{HttpError, PackageNotFound};
use fluvio_cli_common::install::{
    fetch_latest_version, fetch_package_file, fluvio_extensions_dir, install_bin, install_println,
    fluvio_bin_dir,
};

use fluvio_index::{PackageId, HttpAgent, MaybeVersion};
use fluvio_channel::{LATEST_CHANNEL_NAME, FLUVIO_RELEASE_CHANNEL};
use fluvio_hub_util as hubutil;
use hubutil::{http, HubAccess, INFINYON_HUB_REMOTE, FLUVIO_HUB_PROFILE_ENV};
use hubutil::http::StatusCode;

use crate::error::CliError;
use crate::install::update::{
    check_update_required, prompt_required_update, check_update_available, prompt_available_update,
};

use super::update::should_always_print_available_update;
pub const HUB_API_BPKG_AUTH: &str = "hub/v0/bpkg-auth"; // copied from hub-tool

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
    #[clap(long, hide_short_help = true)]
    pub hub: bool,

    /// Use local hub defaults.
    /// Implied if INFINYON_HUB_REMOTE or FLUVIO_CLOUD_PROFILE env vars are set
    /// - Dev-only
    #[clap(long, hide_short_help = true)]
    pub use_hub_defaults: bool,

    /// When this flag is provided, use the hub. Dev-only
    #[clap(long, hide_short_help = true)]
    pub channel: Option<String>,

    /// When this flag is provided, use the hub. Dev-only
    #[clap(long, hide_short_help = true)]
    pub target: Option<String>,
}

impl InstallOpt {
    pub async fn process(self) -> Result<()> {
        if self.hub {
            debug!("Using the hub to install");

            let mut homedir_path = fluvio_bin_dir()?;
            let package_name;
            //let binpath = "sample-bin-script";
            let bin_install_path = if let Some(ref p) = self.package {
                let package = p;
                package_name = package.name().to_string();
                homedir_path.push(package_name.clone());
                homedir_path.to_str().ok_or(crate::CliError::Other(
                    "Unable to render path to fluvio bin dir".to_string(),
                ))?
            } else {
                return Err(crate::CliError::Other("No package name provided".to_string()).into());
            };
            debug!(?bin_install_path, "Install path");

            let access_remote = if std::env::var(INFINYON_HUB_REMOTE).is_ok()
                || std::env::var(FLUVIO_HUB_PROFILE_ENV).is_ok()
                || self.use_hub_defaults
            {
                None
            } else {
                Some("https://hub.infinyon.cloud".to_string())
            };

            let access = HubAccess::default_load(&access_remote).map_err(|_| {
                crate::CliError::Other("Something happened getting hub dev info".to_string())
            })?;
            let data = self.get_binary(&package_name, &access).await?;

            debug!(?bin_install_path, "Writing binary to fs");
            install_bin(bin_install_path, data)?;
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
                    "❕   https://github.com/infinyon/fluvio/issues/new?title=Support+fluvio-cloud+on+target+{target}"
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
                install_println(format!("🎣 Fetching latest version for package: {id}..."));
                let version = fetch_latest_version(agent, id, &target, self.develop).await?;
                let id = id.clone().into_versioned(version.into());
                install_println(format!(
                    "⏳ Downloading package with latest version: {id}..."
                ));
                id
            }
        };

        // Download the package file from the package registry
        let package_result = fetch_package_file(agent, &id, &target).await;
        let package_file = match package_result {
            Ok(pf) => pf,
            Err(err) => match err.downcast_ref::<PackageNotFound>() {
                Some(PackageNotFound {
                    package,
                    version,
                    target,
                }) => {
                    install_println(format!(
                        "❕ Package {package} is not published at {version} for {target}, skipping"
                    ));
                    return Ok(());
                }
                None => return Err(err),
            },
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

    fn get_channel(&self) -> String {
        if let Some(user_override) = &self.channel {
            user_override.to_string()
        } else if let Ok(channel_name) = std::env::var(FLUVIO_RELEASE_CHANNEL) {
            channel_name
        } else {
            LATEST_CHANNEL_NAME.to_string()
        }
    }

    fn get_target(&self) -> String {
        if let Some(user_override) = &self.target {
            user_override.to_string()
        } else {
            CURRENT_PLATFORM.to_string()
        }
    }

    async fn get_binary(&self, bin_name: &str, access: &HubAccess) -> Result<Vec<u8>> {
        let actiontoken = access
            .get_bpkg_get_token()
            .await
            .map_err(|_| HttpError::InvalidInput("authorization error".into()))?;

        let binurl = format!(
            "{}/{HUB_API_BPKG_AUTH}/{channel}/{systuple}/{bin_name}",
            access.remote,
            channel = self.get_channel(),
            systuple = self.get_target(),
        );
        debug!("Downloading binary from hub: {binurl}");
        let mut resp = http::get(binurl)
            .header("Authorization", actiontoken)
            .await
            .map_err(|_| HttpError::InvalidInput("authorization error".into()))?;

        match resp.status() {
            StatusCode::Ok => {}
            code => {
                let body_err_message = resp
                    .body_string()
                    .await
                    .unwrap_or_else(|_err| "couldn't fetch error message".to_string());
                let msg = format!("Status({code}) {body_err_message}");
                return Err(crate::CliError::HubError(msg).into());
            }
        }
        let data = resp
            .body_bytes()
            .await
            .map_err(|_| crate::CliError::HubError("Data unpack failure".into()))?;
        Ok(data)
    }
}
