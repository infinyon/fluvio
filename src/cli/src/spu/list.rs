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
use crate::common::output::Terminal;
use super::format_spu_response_output;
use crate::common::OutputFormat;
use fluvio::metadata::spu::CustomSpuSpec;
use fluvio::metadata::objects::Metadata;

#[derive(Debug, StructOpt)]
pub struct ListSpusOpt {
    /// Whether to list managed SPUs
    #[structopt(long)]
    managed: bool,
    /// Whether to list custom SPUs
    #[structopt(long)]
    custom: bool,
    /// The output format to print the SPUs
    #[structopt(flatten)]
    output: OutputFormat,
}

impl ListSpusOpt {
    /// Process list spus cli request
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        let mut admin = fluvio.admin().await;

        let (managed, custom) = match (self.managed, self.custom) {
            // If neither --managed nor --custom is given, list both
            (false, false) => (true, true),
            // Otherwise, list the SPUs for whichever flags are given
            other => other,
        };

        let mut spus = vec![];

        if managed {
            let managed_spus = admin.list::<SpuSpec, _>(vec![]).await?;
            spus.extend(managed_spus);
        }

        if custom {
            let custom_spus = admin.list::<CustomSpuSpec, _>(vec![]).await?;
            let custom_spus: Vec<Metadata<SpuSpec>> = custom_spus
                .into_iter()
                .map(|custom_spu| Metadata {
                    name: custom_spu.name,
                    spec: custom_spu.spec.into(),
                    status: custom_spu.status,
                })
                .collect();
            spus.extend(custom_spus);
        }

        // format and dump to screen
        format_spu_response_output(out, spus, self.output.format)?;
        Ok(())
    }
}
