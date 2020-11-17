#![type_length_limit = "2669460"]

mod http;
mod common;
mod error;
mod consume;
mod produce;
mod root_cli;
mod spu;
mod topic;
mod output;
pub mod profile;
mod tls;
pub mod cluster;
mod group;
mod custom;
mod install;
mod partition;

#[cfg(any(feature = "cluster_components", feature = "cluster_components_rustls"))]
mod run;

pub use self::error::{Result, CliError};

pub use output::Terminal;
use output::*;

pub use root_cli::Root;

const VERSION: &str = include_str!("VERSION");

const COMMAND_TEMPLATE: &str = "{about}

{usage}

{all-args}
";

#[macro_export]
macro_rules! t_print {
    ($out:expr,$($arg:tt)*) => ( $out.print(&format!($($arg)*)))
}

#[macro_export]
macro_rules! t_println {
    ($out:expr,$($arg:tt)*) => ( $out.println(&format!($($arg)*)))
}

#[macro_export]
macro_rules! t_print_cli_err {
    ($out:expr,$x:expr) => {
        t_println!($out, "\x1B[1;31merror:\x1B[0m {}", $x);
    };
}

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
