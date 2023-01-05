//!
//! # List All Spus CLI
//!
//! CLI tree and processing to list SPUs
//!

use std::sync::Arc;

use clap::Parser;
use anyhow::Result;

use fluvio::Fluvio;
use fluvio_controlplane_metadata::spu::SpuSpec;
use fluvio::metadata::customspu::CustomSpuSpec;
use fluvio::metadata::objects::Metadata;

use crate::cli::common::output::Terminal;
use crate::cli::common::OutputFormat;
use crate::cli::spu::display::format_spu_response_output;

#[derive(Debug, Parser)]
pub struct ListSpusOpt {
    /// Whether to list only custom SPUs
    #[clap(long)]
    custom: bool,
    /// The output format to print the SPUs
    #[clap(flatten)]
    output: OutputFormat,
}

impl ListSpusOpt {
    /// Process list spus cli request
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        let admin = fluvio.admin().await;

        let spus = if self.custom {
            // List custom SPUs only
            admin
                .all::<CustomSpuSpec>()
                .await?
                .into_iter()
                .map(|custom_spu| Metadata {
                    name: custom_spu.name,
                    spec: custom_spu.spec.into(),
                    status: custom_spu.status,
                })
                .collect()
        } else {
            // List all SPUs
            admin.all::<SpuSpec>().await?
        };

        // format and dump to screen
        format_spu_response_output(out, spus, self.output.format)?;
        Ok(())
    }
}
