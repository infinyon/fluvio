use clap::Parser;
use anyhow::Result;

use fluvio::config::{ConfigFile, TlsPolicy};
use fluvio_extension_common::installation::InstallationType;

#[derive(Debug, Parser)]
pub struct ManualAddOpt {
    /// Name of profile to add
    profile_name: String,

    /// address of cluster, e.g. 127.0.0.1:9003
    cluster_address: String,

    /// Installation type of cluster, e.g. local, local-k8, k8
    installation_type: Option<InstallationType>,
}

impl ManualAddOpt {
    pub fn process(self) -> Result<()> {
        let mut config_file = match ConfigFile::load(None) {
            Ok(config_file) => config_file,
            Err(_) => {
                println!("Creating default fluvio config file");
                ConfigFile::default_config()?
            }
        };

        let def_tls = TlsPolicy::Disabled;
        config_file.add_or_replace_profile(&self.profile_name, &self.cluster_address, &def_tls)?;
        let config = config_file.mut_config().current_cluster_mut()?;
        self.installation_type.unwrap_or_default().save_to(config)?;
        config_file.save()?;
        println!("Switched to profile {}", &self.profile_name);

        Ok(())
    }
}
