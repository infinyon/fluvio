//!
//! # List All Spus CLI
//!
//! CLI tree and processing to list SPUs
//!

use structopt::StructOpt;

use flv_client::ClusterConfig;
use flv_metadata::spu::SpuSpec;

use crate::error::CliError;
use crate::OutputType;
use crate::Terminal;
use crate::target::ClusterTarget;
use super::format_spu_response_output;
use crate::common::OutputFormat;

#[derive(Debug)]
pub struct ListSpusConfig {
    pub output: OutputType,
}

#[derive(Debug, StructOpt)]
pub struct ListSpusOpt {
    #[structopt(flatten)]
    output: OutputFormat,

    #[structopt(flatten)]
    target: ClusterTarget,
}

impl ListSpusOpt {
    /// Validate cli options and generate config
    fn validate(self) -> Result<(ClusterConfig, OutputType), CliError> {
        let target_server = self.target.load()?;

        Ok((target_server, self.output.as_output()))
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
    let (target_server, output) = opt.validate()?;

    let mut client = target_server.connect().await?;
    let mut admin = client.admin().await;

    let spus = admin.list::<SpuSpec,_>(vec![]).await?;

    // format and dump to screen
    format_spu_response_output(out, spus, output)?;
    Ok(())
}
