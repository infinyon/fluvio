use std::net::IpAddr;
use std::str::FromStr;

use thiserror::Error;
use semver::Version;
use k8_config::{ConfigError as K8ConfigError, K8Config};
use url::{Url, ParseError};
use crate::helm::{HelmError, HelmClient};

#[derive(Error, Debug)]
pub enum CheckError {
    /// The fluvio-sys chart is not installed
    #[error("The fluvio-sys chart is not installed")]
    MissingSystemChart,

    /// Fluvio is already correctly installed
    #[error("The fluvio-app chart is already installed")]
    AlreadyInstalled,

    /// Need to update minikube context
    #[error("The minikube context is not active or does not match your minikube ip")]
    InvalidMinikubeContext,

    /// There is no current kubernetest context
    #[error("There is no active Kubernetes context")]
    NoActiveKubernetesContext,

    /// Failed to parse kubernetes cluster server URL
    #[error("Failed to parse server url from Kubernetes context")]
    BadKubernetesServerUrl {
        #[from]
        source: ParseError,
    },

    /// There are multiple fluvio-sys's installed
    #[error("Cannot have multiple versions of fluvio-sys installed")]
    MultipleSystemCharts,

    /// The current kubernetes cluster must have a server hostname
    #[error("Missing Kubernetes server host")]
    MissingKubernetesServerHost,

    /// The server address for the current cluster must be a hostname, not an IP
    #[error("Kubernetes server must be a hostname, not an IP address")]
    KubernetesServerIsIp,

    /// The installed version of helm is incompatible
    #[error("Must have helm version {required} or later. You have {installed}")]
    IncompatibleHelmVersion {
        /// The currently-installed helm version
        installed: String,
        /// The minimum required helm version
        required: String,
    },

    /// There was a problem with the helm client during pre-check
    #[error("Helm client error")]
    HelmError {
        #[from]
        source: HelmError,
    },

    #[error("Kubernetes config error")]
    K8ConfigError {
        #[from]
        source: K8ConfigError,
    }
}

// Getting server hostname from K8 context
pub fn check_cluster_server_host() -> Result<(), CheckError> {
    let config = K8Config::load()?;
    let context = match config {
        K8Config::Pod(_) => return Ok(()),
        K8Config::KubeConfig(context) => context,
    };

    let cluster_context = context.config.current_cluster()
        .ok_or(CheckError::NoActiveKubernetesContext)?;
    let server_url = cluster_context.cluster.server.to_owned();
    let url = Url::parse(&server_url)
        .map_err(|source| CheckError::BadKubernetesServerUrl { source })?;
    let host = url.host().ok_or(CheckError::MissingKubernetesServerHost)?.to_string();
    if host.is_empty() {
        return Err(CheckError::MissingKubernetesServerHost);
    }
    if IpAddr::from_str(&host).is_ok() {
        return Err(CheckError::KubernetesServerIsIp);
    }

    Ok(())
}

/// Checks that the installed helm version is compatible with the installer requirements
pub fn check_helm_version(helm: &HelmClient, required: &str) -> Result<(), CheckError> {
    let helm_version = helm.get_helm_version()?;
    if Version::parse(&helm_version) < Version::parse(required) {
        return Err(CheckError::IncompatibleHelmVersion {
            installed: helm_version,
            required: required.to_string(),
        });
    }
    Ok(())
}

/// Check that the system chart is installed
pub fn check_system_chart(helm: &HelmClient, sys_repo: &str) -> Result<(), CheckError> {

    // check installed system chart version
    let sys_charts = helm.get_installed_chart_by_name(sys_repo)?;
    if sys_charts.is_empty() {
        return Err(CheckError::MissingSystemChart);
    } else if sys_charts.len() > 1 {
        return Err(CheckError::MultipleSystemCharts);
    }
    Ok(())
}

/// Checks that Fluvio is not already installed
pub fn check_already_installed(helm: &HelmClient, app_repo: &str) -> Result<(), CheckError> {
    let app_charts = helm.get_installed_chart_by_name(app_repo)?;
    if !app_charts.is_empty() {
        return Err(CheckError::AlreadyInstalled);
    }
    Ok(())
}
