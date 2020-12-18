//!
//! # List All Spus CLI
//!
//! CLI tree and processing to list SPUs
//!

use std::sync::Arc;
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio_controlplane_metadata::spu::SpuSpec;
use fluvio::metadata::spu::CustomSpuSpec;
use fluvio::metadata::objects::Metadata;

use crate::cli::ClusterCliError;
use crate::cli::common::output::Terminal;
use crate::cli::common::OutputFormat;
use crate::cli::spu::display::format_spu_response_output;

#[derive(Debug, StructOpt)]
pub struct ListSpusOpt {
    /// Whether to list only custom SPUs
    #[structopt(long)]
    custom: bool,
    /// The output format to print the SPUs
    #[structopt(flatten)]
    output: OutputFormat,
}

impl ListSpusOpt {
    /// Process list spus cli request
    pub async fn process<O: Terminal>(
        self,
        out: Arc<O>,
        fluvio: &Fluvio,
    ) -> Result<(), ClusterCliError> {
        let mut admin = fluvio.admin().await;

        let spus = if self.custom {
            // List custom SPUs only
            admin
                .list::<CustomSpuSpec, _>(vec![])
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
            admin.list::<SpuSpec, _>(vec![]).await?
        };

        // format and dump to screen
        format_spu_response_output(out, spus, self.output.format)?;
        Ok(())
    }
}
