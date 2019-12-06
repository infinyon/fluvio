use std::path::Path;
use std::fs::read_to_string;

use log::debug;
use log::trace;
use log::error;

const BASE_DIR: &'static str = "/var/run/secrets/kubernetes.io/serviceaccount";
const API_SERVER: &'static str = "https://kubernetes.default.svc";

///var/run/secrets/kubernetes.io/serviceaccount

/// Configuration as Pod
#[derive(Debug, Default, Clone)]
pub struct PodConfig {
    pub namespace: String,
    pub token: String,
}

impl PodConfig {
    pub fn load() -> Option<Self> {
        // first try to see if this base dir account exists, otherwise return non
        let path = Path::new(BASE_DIR);
        if !path.exists() {
            debug!(
                "pod config dir: {} is not founded, skipping pod config",
                BASE_DIR
            );
            return None;
        }

        let namespace = read_file("namespace")?;
        let token = read_file("token")?;

        Some(Self {
            namespace,
            token,
        })
    }

    pub fn api_path(&self) -> &'static str {
        API_SERVER
    }

    /// path to CA certificate
    pub fn ca_path(&self) -> String {
        format!("{}/{}", BASE_DIR, "ca.crt")
    }

}

// read file
fn read_file(name: &str) -> Option<String> {
    let full_path = format!("{}/{}", BASE_DIR, name);
    match read_to_string(&full_path) {
        Ok(value) => Some(value),
        Err(err) => {
            error!("no {} founded as pod in {}", name,full_path);
            trace!("unable to read pod: {} value: {}", name, err);
            None
        }
    }
}
