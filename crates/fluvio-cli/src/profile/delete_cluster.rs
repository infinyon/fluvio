use clap::Parser;
use anyhow::Result;

use fluvio::config::ConfigFile;

#[derive(Debug, Parser)]
pub struct DeleteClusterOpt {
    /// The name of a cluster connection to delete
    #[clap(value_name = "cluster name")]
    pub cluster_name: String,
    /// Deletes a cluster even if its active
    #[clap(short, long)]
    pub force: bool,
}

impl DeleteClusterOpt {
    pub async fn process(self) -> Result<()> {
        let cluster_name = self.cluster_name;

        let mut config_file = match ConfigFile::load(None) {
            Ok(config_file) => config_file,
            Err(e) => {
                println!("No config can be found: {e}");
                return Ok(());
            }
        };

        let config = config_file.mut_config();

        // Check if the named cluster exists
        if config.cluster(&cluster_name).is_none() {
            println!("No profile named {} exists", &cluster_name);
            return Ok(());
        }

        if !self.force {
            // Check whether there are any profiles that conflict with
            // this cluster being deleted. That is, if any profiles reference it.
            if let Err(profile_conflicts) = config.delete_cluster_check(&cluster_name) {
                println!(
                    "The following profiles reference cluster {}:",
                    &cluster_name
                );
                for profile in profile_conflicts.iter() {
                    println!("  {profile}");
                }
                println!("If you would still like to delete the cluster, use --force");
                return Ok(());
            }
        }

        let _deleted = match config.delete_cluster(&cluster_name) {
            Some(deleted) => deleted,
            None => {
                println!("Cluster {} not found", &cluster_name);
                return Ok(());
            }
        };

        match config_file.save() {
            Ok(_) => {
                println!("Cluster {} deleted", &cluster_name);
            }
            Err(e) => {
                println!("Unable to save config file: {e}");
            }
        }
        Ok(())
    }
}
