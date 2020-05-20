//!
//! # List All Spus CLI
//!
//! CLI tree and processing to list SPUs
//!

use structopt::StructOpt;

use flv_client::profile::ScConfig;

use crate::error::CliError;
use crate::OutputType;
use crate::Terminal;
use crate::spu::helpers::format_spu_response_output;
use crate::tls::TlsConfig;
use crate::profile::InlineProfile;

#[derive(Debug)]
pub struct ListSpusConfig {
    pub output: OutputType,
}

#[derive(Debug, StructOpt)]
pub struct ListSpusOpt {
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
    profile: InlineProfile
}

impl ListSpusOpt {
    /// Validate cli options and generate config
    fn validate(self) -> Result<(ScConfig, ListSpusConfig), CliError> {
        let target_server = ScConfig::new_with_profile(
            self.sc,
            self.tls.try_into_file_config()?,
            self.profile.profile)?;

        // transfer config parameters
        let list_spu_cfg = ListSpusConfig {
            output: self.output.unwrap_or(OutputType::default()),
        };

        // return server separately from topic result
        Ok((target_server, list_spu_cfg))
    }
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process list spus cli request
pub async fn process_list_spus<O>(out: std::sync::Arc<O>, opt: ListSpusOpt) -> Result<(), CliError>
where
    O: Terminal,
{
    let (target_server, list_spu_cfg) = opt.validate()?;

    let mut sc = target_server.connect().await?;

    let spus = sc.list_spu(false).await?;
   
    // format and dump to screen
    format_spu_response_output(out, spus, list_spu_cfg.output)?;
    Ok(())
}
