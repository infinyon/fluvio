//!
//! # Profile Configurations
//!
//! Stores configuration parameter retrieved from the default or custom profile file.
//!
use std::io::Error as IoError;
use std::io::ErrorKind;



use crate::ClientConfig;
use crate::ScClient;
use crate::KfClient;
use crate::ClientError;
use crate::SpuReplicaLeader;
use crate::KfLeader;
use crate::SpuController;
use crate::ReplicaLeaderConfig;

use super::config::Config;
use super::config::Cluster;



/// Configure Sc server which was provided or thru profile
pub struct ScConfig(Cluster);

impl ScConfig {
    pub fn new(domain_option: Option<String>) -> Result<Self, ClientError> {
        
        if let Some(domain) = domain_option {
            Ok(Self(
                domain.into()
            ))
        } else {
            // look up using profile
            let config_file = Config::load(None)?;
            if let Some(cluster) = config_file.config().current_cluster() {
                Ok(Self(cluster.clone()))
            } else {
                Err(IoError::new(ErrorKind::Other,"no current cluster founded in profile").into())
            }
        }
    }

    pub async fn connect(self) -> Result<ScClient, ClientError> {

        let config: ClientConfig = self.0.into();
        Ok(ScClient::new( config.connect().await?))
    }
}

pub struct KfConfig(String);

impl KfConfig {

    pub fn new(domain: String) -> Self {
        Self(domain)
    }

    pub async fn connect(self) -> Result<KfClient, ClientError> {

        let config = ClientConfig::with_domain(self.0);
        Ok(KfClient::new( config.connect().await?))
    }
}

/// Contains either spu leader or kafka
pub enum ReplicaLeaderTarget {
    Spu(SpuReplicaLeader),
    Kf(KfLeader),
}


pub enum SpuControllerTarget {
    Sc(ScClient),
    Kf(KfClient),
}


#[derive(Debug)]
pub enum SpuControllerTargetConfig {
    Sc(Option<String>),
    Kf(String),
}

impl SpuControllerTargetConfig {

    pub fn possible_target(sc: Option<String>,kf: Option<String>) -> Result<Self,ClientError> {

        // profile specific configurations (target server)
        if let Some(sc_server) = sc {
            // check if spu or kf is set
            if kf.is_some() {
                return Err(ClientError::Other("kf must not be specified".to_owned()))
            }
            Ok(Self::Sc(Some(sc_server)))
        } else if let Some(kf_server) = kf {
            Ok(Self::Kf(kf_server))
        } else {
            Ok(Self::Sc(None))
        }
    }

    pub async fn connect(self) -> Result<SpuControllerTarget, ClientError> {
        match self {
            Self::Kf(domain) => KfClient::connect(domain.into())
                .await
                .map(|leader| SpuControllerTarget::Kf(leader)),
            Self::Sc(domain) => {
                let sc_config = ScConfig::new(domain)?;
                let sc_client = sc_config.connect().await?;
                Ok(SpuControllerTarget::Sc(sc_client))
            }
        }
    }
}



/// can target sc/kf/spu
#[derive(Debug)]
pub enum ServerTargetConfig {
    Sc(Option<String>),
    Kf(String),
    Spu(String),
}

impl ServerTargetConfig {

    pub fn possible_target(sc: Option<String>,kf: Option<String>,spu: Option<String>) -> Result<Self,ClientError> {

        // profile specific configurations (target server)
        if let Some(sc_server) = sc {
            // check if spu or kf is set
            if kf.is_some() || spu.is_some() {
                return Err(ClientError::Other("kf and spu must not be specified".to_owned()))
            }
            Ok(Self::sc(Some(sc_server)))
        } else if let Some(kf_server) = kf {
            if spu.is_some() {
                return Err(ClientError::Other("kf and spu must not be specified".to_owned()))
            }
            Ok(Self::kf(kf_server))
        } else if let Some(spu_server) = spu {
            Ok(Self::spu(spu_server))
        } else {
            Ok(Self::sc(None))
        }
    }

    pub fn sc(target: Option<String>) -> Self {
        Self::Sc(target)
    }

    pub fn kf(target: String) -> Self {
        Self::Kf(target)
    }

    pub fn spu(target: String) -> Self {
        Self::Spu(target)
    }
    

    pub async fn connect(
        self,
        topic: &str,
        partition: i32,
    ) -> Result<ReplicaLeaderTarget, ClientError> {

        match self {
            Self::Kf(domain) => {
                let mut kf_client = KfClient::connect(domain.into()).await?;
                kf_client
                    .find_replica_for_topic_partition(topic, partition)
                    .await
                    .map(|leader| ReplicaLeaderTarget::Kf(leader))
            }
            Self::Sc(domain) => {
                let sc_config = ScConfig::new(domain)?;
                let mut sc_client = sc_config.connect().await?;
                sc_client
                    .find_replica_for_topic_partition(topic, partition)
                    .await
                    .map(|leader| ReplicaLeaderTarget::Spu(leader))
            }
            Self::Spu(domain) => {

                let config: ClientConfig = domain.into();   // get generic configuration
                let leader_config = ReplicaLeaderConfig::new(topic.to_owned(), partition);
                Ok(ReplicaLeaderTarget::Spu(SpuReplicaLeader::new(leader_config,config.connect().await?)))
            }
        }
    }
}

