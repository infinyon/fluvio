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
mod partition;

#[cfg(feature = "cluster_components")]
mod run;

pub use self::error::CliError;
pub use self::root_cli::run_cli;

pub use output::Terminal;
use output::*;

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
    use structopt::StructOpt;

    use fluvio::config::ConfigFile;
    use fluvio::ClusterConfig;
    use crate::tls::TlsOpt;
    use crate::CliError;

    #[derive(Debug, StructOpt, Default)]
    pub struct InlineProfile {
        #[structopt(short = "P", long, value_name = "profile")]
        pub profile: Option<String>,
    }

    /// server configuration
    #[derive(Debug, StructOpt, Default)]
    pub struct ClusterTarget {
        /// Address of cluster
        #[structopt(short = "c", long, value_name = "host:port")]
        pub cluster: Option<String>,

        #[structopt(flatten)]
        pub tls: TlsOpt,

        #[structopt(flatten)]
        pub profile: InlineProfile,
    }

    impl ClusterTarget {
        /// try to create sc config
        pub fn load(self) -> Result<ClusterConfig, CliError> {
            let tls = self.tls.try_into_inline()?;

            match (self.profile.profile, self.cluster, tls) {
                // Profile and Cluster together is illegal
                (Some(_profile), Some(_cluster), _) => Err(CliError::invalid_arg(
                    "cluster addr is not valid when profile is used",
                )),
                // Profile and TLS together is illegal
                (Some(_profile), _, Some(_tls)) => Err(CliError::invalid_arg(
                    "tls is not valid when profile is is used",
                )),
                // Using Profile
                (Some(profile), _, _) => {
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
                // Using Cluster with TLS
                (None, Some(cluster), Some(tls)) => {
                    let cluster = ClusterConfig::new(cluster).with_tls(tls);
                    Ok(cluster)
                }
                // TLS is illegal without Cluster
                (None, None, Some(_tls)) => Err(CliError::invalid_arg(
                    "tls is only valid if cluster addr is used",
                )),
                // If no profile or cluster are given, try to use the current cluster
                _ => {
                    let config_file = ConfigFile::load(None)?;
                    let cluster = config_file.config().current_cluster().ok_or_else(|| {
                        IoError::new(ErrorKind::Other, "Current cluster not found")
                    })?;
                    Ok(cluster.clone())
                }
            }
        }
    }
}
