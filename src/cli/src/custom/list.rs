//!
//! # List Custom SPUs CLI
//!
//! CLI tree and processing to list Custom SPUs
//!
use std::sync::Arc;
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio::metadata::spu::CustomSpuSpec;
use fluvio::metadata::spu::SpuSpec;
use fluvio::metadata::objects::Metadata;

use crate::Result;
use crate::Terminal;
use crate::spu::format_spu_response_output;
use crate::common::OutputFormat;

#[derive(Debug, StructOpt)]
pub struct ListCustomSpusOpt {
    #[structopt(flatten)]
    output: OutputFormat,
}

impl ListCustomSpusOpt {
    /// Process list spus cli request
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        let output = self.output.format;
        let mut admin = fluvio.admin().await;
        let custom_spus = admin.list::<CustomSpuSpec, _>(vec![]).await?;

        let spus: Vec<Metadata<SpuSpec>> = custom_spus
            .into_iter()
            .map(|custom_spu| Metadata {
                name: custom_spu.name,
                spec: custom_spu.spec.into(),
                status: custom_spu.status,
            })
            .collect();

        // format and dump to screen
        format_spu_response_output(out, spus, output)?;
        Ok(())
    }
}
