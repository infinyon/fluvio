//!
//! # Fluvio SC - output processing
//!
//! Format SPU response based on output type
//!
use prettytable::Row;
use prettytable::row;
use prettytable::cell;

use crate::error::CliError;
use crate::common::OutputType;
use crate::common::{EncoderOutputHandler, TableOutputHandler};

use super::list_metadata::ScSpuMetadata;

// -----------------------------------
// ListSpus Data Structure
// -----------------------------------

#[derive(Debug)]
struct ListSpus {
    spus: Vec<ScSpuMetadata>,
}

// -----------------------------------
// Process Request
// -----------------------------------

/// Process server based on output type
pub fn format_spu_response_output(
    spus: Vec<ScSpuMetadata>,
    output_type: &OutputType,
) -> Result<(), CliError> {
    let list_spus = ListSpus { spus };

    // expecting array with one or more elements
    if list_spus.spus.len() > 0 {
        if output_type.is_table() {
            list_spus.display_errors();
            list_spus.display_table(false);
        } else {
            list_spus.display_encoding(output_type)?;
        }
    } else {
        println!("No spus found");
    }
    Ok(())
}

// -----------------------------------
// Output Handlers
// -----------------------------------
impl TableOutputHandler for ListSpus {
    /// table header implementation
    fn header(&self) -> Row {
        row!["ID", "NAME", "STATUS", "TYPE", "RACK", "PUBLIC", "PRIVATE"]
    }

    /// return errors in string format
    fn errors(&self) -> Vec<String> {
        let mut errors = vec![];
        for spu_metadata in &self.spus {
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
        for spu_metadata in &self.spus {
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

impl EncoderOutputHandler for ListSpus {
    /// serializable data type
    type DataType = Vec<ScSpuMetadata>;

    /// serializable data to be encoded
    fn data(&self) -> &Vec<ScSpuMetadata> {
        &self.spus
    }
}
