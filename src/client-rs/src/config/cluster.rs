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

use crate::client::*;
use crate::ClientError;

use super::config::ConfigFile;
use super::tls::TlsConfig;

/// public configuration for the cluster
pub struct ClusterConfig {
    addr: String,
    tls: Option<TlsConfig>,
}

impl ClusterConfig {
    /// create cluster configuration if addr and tls are known
    pub fn new<S: Into<String>>(addr: S, tls: Option<TlsConfig>) -> Self {
        Self {
            addr: addr.into(),
            tls,
        }
    }

    /// look up cluster configuration from profile
    pub fn lookup_profile(profile: Option<String>) -> Result<Self, ClientError> {
        // load with default path
        let config_file = ConfigFile::load(None)?;
        if let Some(cluster) = config_file
            .config()
            .current_cluster_or_with_profile(profile.as_ref().map(|p| p.as_ref()))
        {
            debug!("looking up using profile: cluster addr {}", cluster.addr);
            Ok(Self {
                addr: cluster.addr().to_owned(),
                tls: cluster.tls.clone(),
            })
        } else {
            Err(IoError::new(ErrorKind::Other, "no matched cluster found").into())
        }
    }

    pub async fn connect(self) -> Result<ClusterClient, ClientError> {
        let connector = match self.tls {
            None => AllDomainConnector::default_tcp(),
            Some(tls) => TryFrom::try_from(tls)?,
        };
        let config = ClientConfig::new(self.addr, connector);
        let inner_client = config.connect().await?;
        debug!("connected to cluster at: {}", inner_client.config().addr());
        let cluster = ClusterClient::new(inner_client);
        //cluster.start_metadata_watch().await?;
        Ok(cluster)
    }
}
