//!
//! # Fluvio SC - output processing
//!
//! Format SPU response based on output type
//!
use prettytable::Row;
use prettytable::row;
use prettytable::cell;

use flv_client::metadata::spu::SpuMetadata;

use crate::error::CliError;
use crate::OutputType;
use crate::Terminal;
use crate::TableOutputHandler;
use crate::t_println;

type ListSpus = Vec<SpuMetadata>;

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
        let mut errors = vec![];
        for spu_metadata in self.iter() {
            if let Some(error) = &spu_metadata.error {
                errors.push(format!(
                    "Spu '{}': {}",
                    spu_metadata.name,
                    error.to_sentence()
                ));
            }
        }
        errors
    }

    /// table content implementation
    fn content(&self) -> Vec<Row> {
        let mut rows: Vec<Row> = vec![];
        for spu_metadata in self.iter() {
            if let Some(spu) = &spu_metadata.spu {
                rows.push(row![
                    r -> spu.id,
                    l -> spu.name,
                    l -> spu.status_label(),
                    l -> spu.type_label(),
                    c -> (&spu.rack).as_ref().unwrap_or(&"-".to_string()),
                    l -> spu.public_server,
                    l -> spu.private_server,
                ]);
            }
        }
        rows
    }
}
