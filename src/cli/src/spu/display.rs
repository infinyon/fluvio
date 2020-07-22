//!
//! # Fluvio SC - output processing
//!
//! Format SPU response based on output type
//!
use prettytable::Row;
use prettytable::row;
use prettytable::cell;

use flv_client::metadata::objects::Metadata;
use flv_client::metadata::spu::SpuSpec;

use crate::error::CliError;
use crate::OutputType;
use crate::Terminal;
use crate::TableOutputHandler;
use crate::t_println;

type ListSpus = Vec<Metadata<SpuSpec>>;

/// Process server based on output type
pub fn format_spu_response_output<O>(
    out: std::sync::Arc<O>,
    spus: ListSpus,
    output_type: OutputType,
) -> Result<(), CliError>
where
    O: Terminal,
{
    if spus.len() > 0 {
        out.render_list(&spus, output_type)?;
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
        self.iter()
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
