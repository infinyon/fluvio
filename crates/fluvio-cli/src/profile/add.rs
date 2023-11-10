use clap::Parser;
use anyhow::Result;

use fluvio::config::{ConfigFile, TlsPolicy};
use fluvio_cluster::InstallationType;

#[derive(Debug, Parser)]
pub struct ManualAddOpt {
    /// Name of profile to add
    profile_name: String,

    /// Address of cluster, e.g. 127.0.0.1:9003
    cluster_address: String,

    /// Installation type of cluster, e.g. local, local-k8, k8
    installation_type: Option<InstallationType>,
}
// todo: p2 add tls config, p1 is default disabled for manual add

impl ManualAddOpt {
    pub fn process(self) -> Result<()> {
        match ConfigFile::load(None) {
            Ok(mut config_file) => {
                let def_tls = TlsPolicy::Disabled;
                config_file.add_or_replace_profile(
                    &self.profile_name,
                    &self.cluster_address,
                    &def_tls,
                )?;
                let config = config_file.mut_config().current_cluster_mut()?;
                self.installation_type.unwrap_or_default().save_to(config)?;
                config_file.save()?;
                println!("Switched to profile {}", &self.profile_name);
            }
            Err(_) => println!("no profile can be found"),
        }

        Ok(())
    }
}
