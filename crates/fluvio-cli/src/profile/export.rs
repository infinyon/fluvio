use std::sync::Arc;

use clap::Parser;
use anyhow::{anyhow, Result};

use fluvio::config::{ConfigFile, TlsConfig, TlsPolicy};
use fluvio_extension_common::Terminal;
use fluvio_extension_common::output::OutputType;

use crate::error::CliError;

#[derive(Parser, Debug)]
pub struct ExportOpt {
    profile_name: Option<String>,
    #[clap(
        default_value_t = OutputType::json,
        short = 'O',
        long = "output",
        value_name = "type",
        value_enum,
        ignore_case = true
    )]
    pub output_format: OutputType,
}

impl ExportOpt {
    pub fn process<O: Terminal>(self, out: Arc<O>) -> Result<()> {
        let output_format = match self.output_format {
            OutputType::table => {
                eprintln!("Table format is not supported, using JSON instead");
                OutputType::json
            }
            _ => self.output_format,
        };

        let config_file = match ConfigFile::load(None) {
            Ok(config_file) => config_file,
            Err(e) => {
                eprintln!("Unable to find Fluvio config file");
                return Err(e.into());
            }
        };

        let cluster_name = if let Some(ref profile_name) = self.profile_name {
            if let Some(profile) = config_file.config().profile(profile_name) {
                profile.cluster.clone()
            } else {
                return Err(CliError::ProfileNotFoundInConfig(profile_name.to_owned()).into());
            }
        } else if let Ok(profile) = config_file.config().current_profile() {
            profile.cluster.clone()
        } else {
            return Err(CliError::NoActiveProfileInConfig.into());
        };
        let profile_export = if let Some(fluvio_config) =
            config_file.config().cluster(&cluster_name)
        {
            if let TlsPolicy::Verified(TlsConfig::Files(_)) = fluvio_config.tls {
                return Err(anyhow!(
                        "Cluster {cluster_name} uses externals TLS certs. Only inline TLS certs are supported."
                    ));
            }
            fluvio_config
        } else {
            return Err(CliError::ClusterNotFoundInConfig(cluster_name.to_owned()).into());
        };

        Ok(out.render_serde(&profile_export, output_format.into())?)
    }
}
