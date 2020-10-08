//!
//! # List All Spus CLI
//!
//! CLI tree and processing to list SPUs
//!

use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio_controlplane_metadata::spu::SpuSpec;

use crate::error::CliError;
use crate::OutputType;
use crate::Terminal;
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
}

impl ListSpusOpt {
    /// Validate cli options and generate config
    fn validate(self) -> Result<OutputType, CliError> {
        Ok(self.output.as_output())
    }
}

// -----------------------------------
//  CLI Processing
// -----------------------------------

/// Process list spus cli request
pub async fn process_list_spus<O>(
    out: std::sync::Arc<O>,
    fluvio: &Fluvio,
    opt: ListSpusOpt,
) -> Result<(), CliError>
where
    O: Terminal,
{
    let output = opt.validate()?;

    let mut admin = fluvio.admin().await;
    let spus = admin.list::<SpuSpec, _>(vec![]).await?;

    // format and dump to screen
    format_spu_response_output(out, spus, output)?;
    Ok(())
}
