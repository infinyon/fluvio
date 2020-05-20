//!
//! # List Custom SPUs CLI
//!
//! CLI tree and processing to list Custom SPUs
//!
use structopt::StructOpt;

use flv_client::profile::ScConfig;

use crate::error::CliError;
use crate::Terminal;
use crate::OutputType;
use crate::spu::helpers::format_spu_response_output;
use crate::tls::TlsConfig;
use crate::profile::InlineProfile;

#[derive(Debug)]
pub struct ListCustomSpusConfig {
    pub output: OutputType,
}

#[derive(Debug, StructOpt)]
pub struct ListCustomSpusOpt {
    /// Address of Streaming Controller
    #[structopt(short = "c", long = "sc", value_name = "host:port")]
    sc: Option<String>,

    /// Output
    #[structopt(
        short = "O",
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

impl ListCustomSpusOpt {
    /// Validate cli options and generate config
    fn validate(self) -> Result<(ScConfig, ListCustomSpusConfig), CliError> {
        let target_server = ScConfig::new_with_profile(
            self.sc,
            self.tls.try_into_file_config()?,
            self.profile.profile,
        )?;

        // transfer config parameters
        let list_custom_spu_cfg = ListCustomSpusConfig {
            output: self.output.unwrap_or(OutputType::default()),
        };

        // return server separately from topic result
        Ok((target_server, list_custom_spu_cfg))
    }
}

/// Process list spus cli request
pub async fn process_list_custom_spus<O>(
    out: std::sync::Arc<O>,
    opt: ListCustomSpusOpt,
) -> Result<(), CliError>
where
    O: Terminal,
{
    let (target_server, list_custom_spu_cfg) = opt.validate()?;

    let mut sc = target_server.connect().await?;

    let spus = sc.list_spu(true).await?;

    // format and dump to screen
    format_spu_response_output(out, spus, list_custom_spu_cfg.output)?;
    Ok(())
}
