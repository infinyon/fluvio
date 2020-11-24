use serde::{Deserialize, Serialize};
pub mod output;
mod common;

#[cfg(feature = "target")]
pub mod tls;

pub use common::*;

pub const COMMAND_TEMPLATE: &str = "{about}

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

#[derive(Debug, Serialize, Deserialize)]
pub struct FluvioExtensionMetadata {
    pub command: String,
    pub description: String,
    pub version: String,
}

#[cfg(feature = "target")]
pub mod target {
    use std::io::{ErrorKind, Error as IoError};
    use std::convert::TryInto;
    use structopt::StructOpt;

    use fluvio::FluvioConfig;
    use fluvio::FluvioError;
    use fluvio::Fluvio;
    use fluvio::config::ConfigFile;
    use crate::tls::TlsClientOpt;

    #[derive(thiserror::Error, Debug)]
    pub enum TargetError {
        #[error(transparent)]
        IoError {
            #[from]
            source: IoError,
        },
        #[error("Fluvio client error")]
        ClientError {
            #[from]
            source: FluvioError,
        },
        #[error("Invalid argument: {0}")]
        InvalidArg(String),
        #[error("Unknown error: {0}")]
        Other(String),
    }

    impl TargetError {
        pub fn invalid_arg<M: Into<String>>(reason: M) -> Self {
            Self::InvalidArg(reason.into())
        }
    }

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
        /// helper method to connect to fluvio
        pub async fn connect(self) -> Result<Fluvio, TargetError> {
            let fluvio_config = self.load()?;
            Fluvio::connect_with_config(&fluvio_config)
                .await
                .map_err(|err| err.into())
        }

        /// try to create sc config
        pub fn load(self) -> Result<FluvioConfig, TargetError> {
            let tls = self.tls.try_into()?;

            use fluvio::config::TlsPolicy::*;
            match (self.profile, self.cluster) {
                // Profile and Cluster together is illegal
                (Some(_profile), Some(_cluster)) => Err(TargetError::invalid_arg(
                    "cluster addr is not valid when profile is used",
                )),
                (Some(profile), _) => {
                    // Specifying TLS is illegal when also giving a profile
                    if let Anonymous | Verified(_) = tls {
                        return Err(TargetError::invalid_arg(
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
                        return Err(TargetError::invalid_arg(
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
