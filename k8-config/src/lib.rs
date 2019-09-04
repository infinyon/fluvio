
mod config;
mod error;
mod pod;

pub use error::ConfigError;
pub use config::KubeConfig;
pub use pod::PodConfig;

use log::info;

#[derive(Debug)]
pub struct KubeContext {
    pub namespace: String,
    pub api_path: String,
    pub config: KubeConfig
}

#[derive(Debug)]
pub enum K8Config {
    Pod(PodConfig),
    KubeConfig(KubeContext)
}

impl Default for K8Config {
    fn default() -> Self {
        Self::Pod(PodConfig::default())
    }
}

impl K8Config {
    pub fn load() -> Result<Self,ConfigError> {
        if let Some(pod_config) = PodConfig::load() {
            info!("found pod config: {:#?}",pod_config);
            Ok(K8Config::Pod(pod_config))
        } else {
            info!("no pod config is found. trying to read kubeconfig");
            let config =  KubeConfig::from_home()?;
            info!("kube config: {:#?}",config);
            // check if we have current cluster
            
            if let Some(current_cluster) = config.current_cluster() {
                let ctx = config.current_context().unwrap();
                let k8contxt = KubeContext {
                    namespace: ctx.context.namespace().to_owned(),
                    api_path: current_cluster.cluster.server.clone(),
                    config
                };
                Ok(K8Config::KubeConfig(k8contxt))
            } else {
                Err(ConfigError::NoCurrentContext)
            }
            
        }
    }

    pub fn api_path(&self) -> &str {
        match self {
            Self::Pod(pod) => pod.api_path(),
            Self::KubeConfig(config) => &config.api_path
        }
    }

    pub fn namespace(&self) -> &str {
        match self {
            Self::Pod(pod) => &pod.namespace,
            Self::KubeConfig(config) => &config.namespace
        }
    }
    
}