use std::convert::TryInto;
use eyre::Context;

use fluvio::config::*;

use crate::{Terminal, CliError};
use crate::t_println;
use crate::profile::sync::LocalOpt;

/// create new local cluster and profile
pub fn set_local_context(local_config: LocalOpt) -> Result<String, CliError> {
    let local_addr = local_config.local;
    let mut config_file = ConfigFile::load_default_or_new()?;

    let config = config_file.mut_config();

    // check if local cluster exists otherwise, create new one
    match config.cluster_mut(LOCAL_PROFILE) {
        Some(cluster) => {
            cluster.addr = local_addr.clone();
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

    Ok(format!("local context is set to: {}", local_addr))
}

pub fn view_profile<O>(out: std::sync::Arc<O>) -> eyre::Result<()>
where
    O: Terminal,
{
    let config_file = ConfigFile::load(None).context("Failed to read Fluvio config file")?;
    t_println!(out, "{:#?}", config_file.config());
    Ok(())
}

pub fn display_current_profile<O>(out: std::sync::Arc<O>)
where
    O: Terminal,
{
    match ConfigFile::load(None) {
        Ok(config_file) => {
            if let Some(profile) = config_file.config().current_profile_name() {
                t_println!(out, "{}", profile);
            } else {
                t_println!(out, "no current profile set");
            }
        }
        Err(_) => t_println!(out, "no profile can be founded"),
    }
}
