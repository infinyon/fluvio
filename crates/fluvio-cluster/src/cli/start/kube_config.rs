use serde::Deserialize;
use thiserror::Error;
use tracing::debug;
use std::process::Command;

#[derive(Deserialize)]
pub(crate) struct KubeConfig {
    pub clusters: Vec<ClusterConfigOuter>,
    pub users: Vec<UserConfigOuter>,
}

#[derive(Deserialize)]
pub(crate) struct ClusterConfigOuter {
    pub cluster: ClusterConfig,
}

#[derive(Deserialize)]
pub(crate) struct UserConfigOuter {
    pub user: UserConfig,
}

#[derive(Deserialize)]
pub(crate) struct ClusterConfig {
    #[serde(rename = "certificate-authority-data")]
    pub ca_cert: String,
}

#[derive(Deserialize)]
pub(crate) struct UserConfig {
    #[serde(rename = "client-certificate-data")]
    pub client_cert: String,
    #[serde(rename = "client-key-data")]
    pub client_key: String,
}

#[derive(Error, Debug)]
pub enum KubeConfigError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error("Error while reading kubeconfig")]
    JsonError(#[from] serde_json::Error),
    #[error("UTF-8 error while reading kubeconfig")]
    Utf8Error(#[from] std::str::Utf8Error),
    #[error("No clusters listed in the kubeconfig")]
    MissingClusters,
    #[error("No users listed in the kubeconfig")]
    MissingUsers,
}

pub(crate) fn read_kube_config() -> Result<KubeConfig, KubeConfigError> {
    let output = Command::new("kubectl")
        .arg("config")
        .arg("view")
        .arg("--minify")
        .arg("--output=json")
        .arg("--raw")
        .output()?;
    let output = std::str::from_utf8(&output.stdout)?;
    debug!("kubeconfig: {output}");
    let kube_config: KubeConfig = serde_json::from_str(output)?;
    Ok(kube_config)
}
