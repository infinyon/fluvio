//!
//! # List All Spus CLI
//!
//! CLI tree and processing to list SPUs
//!

use std::sync::Arc;
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio_controlplane_metadata::spu::SpuSpec;

use crate::Result;
use crate::Terminal;
use super::format_spu_response_output;
use crate::common::OutputFormat;

#[derive(Debug, StructOpt)]
pub struct ListSpusOpt {
    #[structopt(flatten)]
    output: OutputFormat,
}

impl ListSpusOpt {
    /// Process list spus cli request
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        let mut admin = fluvio.admin().await;
        let spus = admin.list::<SpuSpec, _>(vec![]).await?;

        // format and dump to screen
        format_spu_response_output(out, spus, self.output.format)?;
        Ok(())
    }
}
