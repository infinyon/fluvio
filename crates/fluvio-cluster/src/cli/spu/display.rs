//!
//! # Fluvio SC - output processing
//!
//! Format SPU response based on output type
//!
use comfy_table::{Row, Cell};
use serde::Serialize;

use fluvio::metadata::objects::Metadata;
use fluvio::metadata::spu::SpuSpec;

use crate::cli::ClusterCliError;
use crate::cli::common::output::{OutputType, Terminal, TableOutputHandler};
use crate::cli::common::t_println;

#[derive(Serialize)]
struct ListSpus(Vec<Metadata<SpuSpec>>);

/// Process server based on output type
pub fn format_spu_response_output<O>(
    out: std::sync::Arc<O>,
    spus: Vec<Metadata<SpuSpec>>,
    output_type: OutputType,
) -> Result<(), ClusterCliError>
where
    O: Terminal,
{
    if !spus.is_empty() {
        let spu_list = ListSpus(spus);
        out.render_list(&spu_list, output_type)?;
    } else {
        t_println!(out, "no spu");
    }

    Ok(())
}

impl TableOutputHandler for ListSpus {
    /// table header implementation
    fn header(&self) -> Row {
        row!["ID", "NAME", "STATUS", "TYPE", "RACK", "PUBLIC", "PRIVATE"]
    }

    /// return errors in string format
    fn errors(&self) -> Vec<String> {
        vec![]
    }

    fn content(&self) -> Vec<Row> {
        self.0
            .iter()
            .map(|metadata| {
                let spu = &metadata.spec;

                row![
                    r -> spu.id,
                    l -> metadata.name,
                    l -> metadata.status.to_string(),
                    l -> spu.spu_type.to_string(),
                    c -> (&spu.rack).as_ref().unwrap_or(&"-".to_string()),
                    l -> spu.public_endpoint.to_string(),
                    l -> spu.private_endpoint.to_string()
                ]
            })
            .collect()
    }
}
