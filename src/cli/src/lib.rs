mod http;
mod error;
mod root_cli;
pub mod profile;
mod tls;


pub use self::error::{Result, CliError};

use fluvio_extension_common as common;

pub use root_cli::Root;

const VERSION: &str = include_str!("VERSION");

mod target {
    use std::io::{ErrorKind, Error as IoError};
    use std::convert::TryInto;
    use structopt::StructOpt;

    use fluvio::FluvioConfig;
    use fluvio::config::ConfigFile;
    use crate::tls::TlsClientOpt;
    use crate::CliError;

    /// server configuration
    #[derive(Debug, StructOpt, Default)]
    pub struct ClusterTarget {
        /// Address of cluster
        #[structopt(short = "c", long, value_name = "host:port")]
        pub cluster: Option<String>,

        #[structopt(flatten)]
        pub tls: TlsClientOpt,

        #[structopt(short = "P", long, value_name = "profile")]
        pub profile: Option<String>,
    }

    impl ClusterTarget {
        /// try to create sc config
        pub fn load(self) -> Result<FluvioConfig, CliError> {
            let tls = self.tls.try_into()?;

            use fluvio::config::TlsPolicy::*;
            match (self.profile, self.cluster) {
                // Profile and Cluster together is illegal
                (Some(_profile), Some(_cluster)) => Err(CliError::invalid_arg(
                    "cluster addr is not valid when profile is used",
                )),
                (Some(profile), _) => {
                    // Specifying TLS is illegal when also giving a profile
                    if let Anonymous | Verified(_) = tls {
                        return Err(CliError::invalid_arg(
                            "tls is not valid when profile is is used",
                        ));
                    }

                    let config_file = ConfigFile::load(None)?;
                    let cluster = config_file
                        .config()
                        // NOTE: This will not fallback to current cluster like it did before
                        // Current cluster will be used when no profile is given.
                        .cluster_with_profile(&profile)
                        .ok_or_else(|| {
                            IoError::new(ErrorKind::Other, "Cluster not found for profile")
                        })?;
                    Ok(cluster.clone())
                }
                (None, Some(cluster)) => {
                    let cluster = FluvioConfig::new(cluster).with_tls(tls);
                    Ok(cluster)
                }
                (None, None) => {
                    // TLS specification is illegal without Cluster
                    if let Anonymous | Verified(_) = tls {
                        return Err(CliError::invalid_arg(
                            "tls is only valid if cluster addr is used",
                        ));
                    }

                    // Try to use the default cluster from saved config
                    let config_file = ConfigFile::load(None)?;
                    let cluster = config_file.config().current_cluster()?;
                    Ok(cluster.clone())
                }
            }
        }
    }
}
