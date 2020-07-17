//!
//! # List Custom SPUs CLI
//!
//! CLI tree and processing to list Custom SPUs
//!
use structopt::StructOpt;

use flv_client::ClusterConfig;
use flv_client::metadata::spu::CustomSpuSpec;
use flv_client::metadata::spu::SpuSpec;
use flv_client::metadata::objects::Metadata;

use crate::error::CliError;
use crate::Terminal;
use crate::OutputType;
use crate::spu::format_spu_response_output;
use crate::target::ClusterTarget;
use crate::common::OutputFormat;

#[derive(Debug)]
pub struct ListCustomSpusConfig {
    pub output: OutputType,
}

#[derive(Debug, StructOpt)]
pub struct ListCustomSpusOpt {
    #[structopt(flatten)]
    output: OutputFormat,

    #[structopt(flatten)]
    target: ClusterTarget,
}

impl ListCustomSpusOpt {
    /// Validate cli options and generate config
    fn validate(self) -> Result<(ClusterConfig, OutputType), CliError> {
        let target_server = self.target.load()?;

        Ok((target_server, self.output.as_output()))
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
    let (target_server, output_type) = opt.validate()?;

    let mut client = target_server.connect().await?;
    let mut admin = client.admin().await;

    let custom_spus = admin.list::<CustomSpuSpec,_>(vec![]).await?;

    let spus: Vec<Metadata<SpuSpec>> = custom_spus
        .into_iter()
        .map(|custom_spu| Metadata {
            name: custom_spu.name,
            spec: custom_spu.spec.into(),
            status: custom_spu.status,
        })
        .collect();

    // format and dump to screen
    format_spu_response_output(out, spus, output_type)?;
    Ok(())
}
