//!
//! # Fluvio SC - output processing
//!
//! Format SPU Group response based on output type

use prettytable::Row;
use prettytable::row;
use prettytable::Cell;
use prettytable::cell;
use prettytable::format::Alignment;
use serde::Serialize;
use log::debug;

use sc_api::server::spu::FlvFetchSpuGroupsResponse;
use k8_metadata::spg::SpuGroupSpec;
use k8_metadata::spg::SpuGroupStatus;

use crate::error::CliError;
use crate::output::OutputType;
use crate::TableOutputHandler;
use crate::Terminal;
use crate::t_println;

#[derive(Serialize, Debug)]
pub struct SpuGroupRow {
    pub name: String,
    pub spec: SpuGroupSpec,
    pub status: SpuGroupStatus,
}

impl SpuGroupRow {
    fn name(&self) -> String {
        self.name.clone()
    }

    fn replicas(&self) -> String {
        self.spec.replicas.to_string()
    }

    fn min_id(&self) -> String {
        self.spec.min_id().to_string()
    }

    fn rack(&self) -> String {
        self.spec
            .template
            .spec
            .rack
            .clone()
            .unwrap_or("".to_string())
    }

    fn size(&self) -> String {
        self.spec.template.spec.storage.clone().unwrap().size()
    }

    fn status(&self) -> String {
        self.status.resolution.to_string()
    }
}

type ListSpuGroups = Vec<SpuGroupRow>;

// -----------------------------------
// Format Output
// -----------------------------------

/// Format SPU Group based on output type
pub fn spu_group_response_to_output<O: Terminal>(
    out: std::sync::Arc<O>,
    spu_groups: FlvFetchSpuGroupsResponse,
    output_type: OutputType,
) -> Result<(), CliError> {
    let groups = spu_groups.spu_groups;

    // TODO: display error output

    let list_spu_groups: Vec<SpuGroupRow> = groups
        .into_iter()
        .map(|g| {
            let (name, spec, status) = g.into();
            SpuGroupRow { name, spec, status }
        })
        .collect();

    debug!("groups: {:#?}", list_spu_groups);

    if list_spu_groups.len() > 0 {
        out.render_list(&list_spu_groups, output_type)
    } else {
        t_println!(out, "no groups");
        Ok(())
    }
}

// -----------------------------------
// Output Handlers
// -----------------------------------
impl TableOutputHandler for ListSpuGroups {
    /// table header implementation
    fn header(&self) -> Row {
        row!["NAME", "REPLICAS", "MIN ID", "RACK", "SIZE", "STATUS",]
    }

    /// return errors in string format
    fn errors(&self) -> Vec<String> {
        self.iter().map(|_g| "".to_owned()).collect()
    }

    /// table content implementation
    fn content(&self) -> Vec<Row> {
        self.iter()
            .map(|r| {
                Row::new(vec![
                    Cell::new_align(&r.name(), Alignment::RIGHT),
                    Cell::new_align(&r.replicas(), Alignment::CENTER),
                    Cell::new_align(&r.min_id(), Alignment::RIGHT),
                    Cell::new_align(&r.rack(), Alignment::RIGHT),
                    Cell::new_align(&r.size(), Alignment::RIGHT),
                    Cell::new_align(&r.status(), Alignment::RIGHT),
                ])
            })
            .collect()
    }
}
