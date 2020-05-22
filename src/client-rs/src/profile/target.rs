//!
//! # Profile Configurations
//!
//! Stores configuration parameter retrieved from the default or custom profile file.
//!
use std::io::Error as IoError;
use std::io::ErrorKind;
use std::convert::TryFrom;

use log::debug;

use flv_future_aio::net::tls::AllDomainConnector;

use crate::ClientConfig;
use crate::ScClient;
use crate::KfClient;
use crate::ClientError;
use crate::SpuReplicaLeader;
use crate::KfLeader;
use crate::SpuController;
use crate::ReplicaLeaderConfig;


use super::config::ConfigFile;
use super::tls::TlsConfig;


/// Logical look for Sc
pub struct ScConfig {
    addr: String,
    tls: Option<TlsConfig>
}

impl ScConfig {
    /// create new Sc with optional address and tls, assume default profile
    pub fn new(
        addr_option: Option<String>, 
        tls: Option<TlsConfig>
    ) -> Result<Self, ClientError> {
        
        Self::new_with_profile(addr_option,tls,None)
    }

    // create new Sc with optional address and tls and can specify optional preferred profile
    pub fn new_with_profile(addr_option: Option<String>, tls: Option<TlsConfig>,profile: Option<String>) -> Result<Self, ClientError> {
        
        if let Some(addr) = addr_option {
            Ok(Self {
                addr,
                tls
            })
        } else {
            // look up using profile
            let config_file = ConfigFile::load(None)?;
            if let Some(cluster) = config_file.config().current_cluster_or_with_profile(profile.as_ref().map(|p| p.as_ref())) {
                Ok(Self {
                    addr: cluster.addr().to_owned(),
                    tls: cluster.tls.clone()
                })
            } else {
                Err(IoError::new(ErrorKind::Other,"no matched cluster found").into())
            }
        }
    }

    pub async fn connect(self) -> Result<ScClient, ClientError> {

        let connector = match self.tls {
            None => AllDomainConnector::default_tcp(),
            Some(tls) => TryFrom::try_from(tls)?
        };
        let config = ClientConfig::new(self.addr,connector);
        Ok(ScClient::new( config.connect().await?))
    }
}

pub struct KfConfig(String);

impl KfConfig {

    pub fn new(domain: String) -> Self {
        Self(domain)
    }

    pub async fn connect(self) -> Result<KfClient, ClientError> {

        let config = ClientConfig::with_addr(self.0);
        Ok(KfClient::new( config.connect().await?))
    }
}

/// Contains either spu leader or kafka
pub enum ReplicaLeaderTargetInstance {
    Spu(SpuReplicaLeader),
    Kf(KfLeader),
}


/// actual controller instances
pub enum ControllerTargetInstance {
    Sc(ScClient),
    Kf(KfClient),
}


/// Controller which can be SC or Kf
#[derive(Debug)]
pub enum ControllerTargetConfig {
    Sc(Option<String>,Option<TlsConfig>,Option<String>),    // custom addr, tls, profile
    Kf(String),
}

impl ControllerTargetConfig {

    pub fn possible_target(
        sc: Option<String>,
        kf: Option<String>,
        tls: Option<TlsConfig>, 
        profile: Option<String>) ->
         Result<Self,ClientError> {

        // check for specific targets
        if let Some(sc_server) = sc {
            // check if spu or kf is set
            if kf.is_some() {
                return Err(ClientError::Other("kf must not be specified".to_owned()))
            }
            Ok(Self::Sc(Some(sc_server),tls,None))
        } else if let Some(kf_server) = kf {
            if tls.is_some() {
                return Err(ClientError::Other("tls is not allowed with kf ".to_owned()))
            }
            Ok(Self::Kf(kf_server))
        } else {
            debug!("no target, looking for profile");
            Ok(Self::Sc(None,tls,profile))
        }
    }

    pub async fn connect(self) -> Result<ControllerTargetInstance, ClientError> {
        match self {
            Self::Kf(addr) => KfClient::connect(addr.into())
                .await
                .map(|leader| ControllerTargetInstance::Kf(leader)),
            Self::Sc(addr,tls,profile) => {
                let sc_config = ScConfig::new_with_profile(addr,tls,profile)?;
                let sc_client = sc_config.connect().await?;
                Ok(ControllerTargetInstance::Sc(sc_client))
            }
        }
    }
}



/// can target sc/kf/spu
#[derive(Debug)]
pub enum ServerTargetConfig {
    Sc(Option<String>,Option<TlsConfig>,Option<String>),
    Kf(String),
    Spu(String,Option<TlsConfig>),
}

impl ServerTargetConfig {

    // not type checked, do dynamic check, should be refactor later on
    pub fn possible_target(
        sc: Option<String>,
        kf: Option<String>,
        spu: Option<String>,
        tls: Option<TlsConfig>,
        profile: Option<String>
    ) -> Result<Self,ClientError> {


        // manual targets
        if let Some(sc_server) = sc {
            // check if spu or kf is set
            if kf.is_some() || spu.is_some() {
                return Err(ClientError::Other("kf and spu must not be specified".to_owned()))
            }
            return Ok(Self::Sc(Some(sc_server),tls,None))
        }
        
        
        if let Some(kf_server) = kf {

            if tls.is_some() {
                return Err(ClientError::Other("tls is not allowed ".to_owned()))
            }
            if spu.is_some() {
                return Err(ClientError::Other("kf and spu must not be specified".to_owned()))
            }
            Ok(Self::kf(kf_server))
        } else if let Some(spu_server) = spu {
            Ok(Self::Spu(spu_server,tls))
        } else {
            Ok(Self::Sc(None,tls,profile))
        }
    }

    pub fn sc(target: Option<String>,profile: Option<String>) -> Self {
        Self::Sc(target,None,profile)
    }

    pub fn kf(target: String) -> Self {
        Self::Kf(target)
    }

    pub fn spu(target: String) -> Self {
        Self::Spu(target,None)
    }
    

    pub async fn connect(
        self,
        topic: &str,
        partition: i32,
    ) -> Result<ReplicaLeaderTargetInstance, ClientError> {

        match self {
            Self::Kf(domain) => {
                let mut kf_client = KfClient::connect(domain.into()).await?;
                kf_client
                    .find_replica_for_topic_partition(topic, partition)
                    .await
                    .map(|leader| ReplicaLeaderTargetInstance::Kf(leader))
            }
            Self::Sc(domain,tls,profile) => {
                let sc_config = ScConfig::new_with_profile(domain,tls,profile)?;
                let mut sc_client = sc_config.connect().await?;
                sc_client
                    .find_replica_for_topic_partition(topic, partition)
                    .await
                    .map(|leader| ReplicaLeaderTargetInstance::Spu(leader))
            }
            Self::Spu(domain,_tls) => {

                let config: ClientConfig = domain.into();   // get generic configuration
                let leader_config = ReplicaLeaderConfig::new(topic.to_owned(), partition);
                Ok(ReplicaLeaderTargetInstance::Spu(SpuReplicaLeader::new(leader_config,config.connect().await?)))
            }
        }
    }
}

