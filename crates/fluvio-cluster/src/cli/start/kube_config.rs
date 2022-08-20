use serde::Deserialize;
use thiserror::Error;
use std::process::Command;

#[derive(Deserialize)]
pub(crate) struct KubeConfig {
    pub clusters: Vec<ClusterConfig>,
    pub users: Vec<UserConfig>,
}

#[derive(Deserialize)]
pub(crate) struct ClusterConfig {
    pub certificate_authority_data: String,
}

#[derive(Deserialize)]
pub(crate) struct UserConfig {
    pub client_certificate_data: String,
    pub client_key_data: String,
}

#[derive(Error, Debug)]
pub enum KubeConfigError {
    #[error(transparent)]
    IoError(#[from] std::io::Error),
    #[error(transparent)]
    JsonError(#[from] serde_json::Error),
    #[error("UTF-8 error when reading kubeconfig: {0}")]
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
    let kube_config: KubeConfig = serde_json::from_str(output)?;
    Ok(kube_config)
}
