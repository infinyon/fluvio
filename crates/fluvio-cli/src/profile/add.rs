use clap::Parser;
use anyhow::Result;

use fluvio::config::{ConfigFile, TlsPolicy, ClusterKind};

#[derive(Debug, Parser)]
pub struct ManualAddOpt {
    /// Name of profile to add
    profile_name: String,

    /// address of cluster, e.g. 127.0.0.1:9003
    cluster_address: String,

    // kind of cluster: local or k8s
    kind: ClusterKind,
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
                    self.kind,
                )?;
                if config_file
                    .mut_config()
                    .set_current_profile(&self.profile_name)
                {
                    println!("Switched to profile {}", &self.profile_name);
                } else {
                    return Err(anyhow::anyhow!("error creating profile"));
                }
            }
            Err(_) => println!("no profile can be found"),
        }

        Ok(())
    }
}
