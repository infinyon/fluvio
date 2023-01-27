use std::convert::TryInto;

use clap::Parser;
use anyhow::Result;

use fluvio::FluvioConfig;
use fluvio::config::{ConfigFile, LOCAL_PROFILE, Profile};

use crate::common::tls::TlsClientOpt;

#[derive(Debug, Default, Parser)]
pub struct LocalOpt {
    #[clap(value_name = "host:port", default_value = "localhost:9003")]
    pub local: String,

    #[clap(flatten)]
    pub tls: TlsClientOpt,
}

impl LocalOpt {
    pub async fn process(self) -> Result<()> {
        match set_local_context(self) {
            Ok(msg) => {
                println!("{msg}");
            }
            Err(err) => {
                eprintln!("config creation failed: {err}");
            }
        }
        Ok(())
    }
}

/// create new local cluster and profile
pub fn set_local_context(local_config: LocalOpt) -> Result<String> {
    let local_addr = local_config.local;
    let mut config_file = ConfigFile::load_default_or_new()?;

    let config = config_file.mut_config();

    // check if local cluster exists otherwise, create new one
    match config.cluster_mut(LOCAL_PROFILE) {
        Some(cluster) => {
            cluster.endpoint = local_addr.clone();
            cluster.tls = local_config.tls.try_into()?;
        }
        None => {
            let mut local_cluster = FluvioConfig::new(local_addr.clone());
            local_cluster.tls = local_config.tls.try_into()?;
            config.add_cluster(local_cluster, LOCAL_PROFILE.to_owned());
        }
    };

    // check if we local profile exits otherwise, create new one, then set it's cluster
    match config.profile_mut(LOCAL_PROFILE) {
        Some(profile) => {
            profile.set_cluster(LOCAL_PROFILE.to_owned());
        }
        None => {
            let profile = Profile::new(LOCAL_PROFILE.to_owned());
            config.add_profile(profile, LOCAL_PROFILE.to_owned());
        }
    }

    // finally we set current profile to local
    assert!(config.set_current_profile(LOCAL_PROFILE));

    config_file.save()?;

    Ok(format!("local context is set to: {local_addr}"))
}
