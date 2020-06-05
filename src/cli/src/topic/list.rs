//!
//! # List Topics CLI
//!
//! CLI tree and processing to list Topics
//!

use structopt::StructOpt;

use log::debug;

use flv_client::profile::ControllerTargetConfig;
use flv_client::profile::ControllerTargetInstance;
use crate::Terminal;
use crate::error::CliError;
use crate::OutputType;
use crate::tls::TlsConfig;
use crate::profile::InlineProfile;

use super::helpers::list_kf_topics;
use super::helpers::list_sc_topics;

// -----------------------------------
//  Parsed Config
// -----------------------------------

#[derive(Debug)]
pub struct ListTopicsConfig {
    pub output: OutputType,
}

// -----------------------------------
// CLI Options
// -----------------------------------

#[derive(Debug, StructOpt)]
pub struct ListTopicsOpt {
    /// Address of Streaming Controller
    #[structopt(short = "c", long = "sc", value_name = "host:port")]
    sc: Option<String>,

    #[structopt(flatten)]
    kf: crate::common::KfConfig,

    /// Output
    #[structopt(
        short = "o",
        long = "output",
        value_name = "type",
        possible_values = &OutputType::variants(),
        case_insensitive = true
    )]
    output: Option<OutputType>,

    #[structopt(flatten)]
    tls: TlsConfig,

    #[structopt(flatten)]
    profile: InlineProfile,
}

impl ListTopicsOpt {
    /// Validate cli options and generate config
    fn validate(self) -> Result<(ControllerTargetConfig, ListTopicsConfig), CliError> {
        let target_server = ControllerTargetConfig::possible_target(
            self.sc,
            #[cfg(kf)]
            self.kf.kf,
            #[cfg(not(foo))]
            None,
            self.tls.try_into_file_config()?,
            self.profile.profile,
        )?;

        // transfer config parameters
        let list_topics_cfg = ListTopicsConfig {
            output: self.output.unwrap_or(OutputType::default()),
        };

        // return server separately from topic result
        Ok((target_server, list_topics_cfg))
    }
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process list topics cli request
pub async fn process_list_topics<O>(
    out: std::sync::Arc<O>,
    opt: ListTopicsOpt,
) -> Result<String, CliError>
where
    O: Terminal,
{
    let (target_server, cfg) = opt.validate()?;

    debug!("list topics {:#?} server: {:#?}", cfg, target_server);

    (match target_server.connect().await? {
        ControllerTargetInstance::Kf(client) => list_kf_topics(out, client, cfg.output).await,
        ControllerTargetInstance::Sc(client) => list_sc_topics(out, client, cfg.output).await,
    })
    .map(|_| format!(""))
    .map_err(|err| err.into())
}
