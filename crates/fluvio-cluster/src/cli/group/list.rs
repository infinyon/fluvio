//! # List SPU Groups CLI
//!
//! CLI tree and processing to list SPU Groups
//!

use std::sync::Arc;

use clap::Parser;
use anyhow::Result;

use fluvio::Fluvio;
use fluvio_controlplane_metadata::spg::SpuGroupSpec;

use crate::cli::common::output::Terminal;
use crate::cli::common::OutputFormat;

#[derive(Debug, Parser)]
pub struct ListManagedSpuGroupsOpt {
    #[clap(flatten)]
    output: OutputFormat,
}

impl ListManagedSpuGroupsOpt {
    /// Process list spus cli request
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        let admin = fluvio.admin().await;
        let lists = admin.all::<SpuGroupSpec>().await?;

        output::spu_group_response_to_output(out, lists, self.output.format)
    }
}

mod output {

    //!
    //! # Fluvio SC - output processing
    //!
    //! Format SPU Group response based on output type

    use comfy_table::{Cell, Row};
    use comfy_table::CellAlignment;
    use tracing::debug;
    use serde::Serialize;
    use anyhow::Result;

    use fluvio::metadata::objects::Metadata;
    use fluvio_controlplane_metadata::spg::SpuGroupSpec;

    use crate::cli::common::output::{OutputType, TableOutputHandler, Terminal};
    use crate::cli::common::t_println;

    #[derive(Serialize)]
    struct ListSpuGroups(Vec<Metadata<SpuGroupSpec>>);

    // -----------------------------------
    // Format Output
    // -----------------------------------

    /// Format SPU Group based on output type
    pub fn spu_group_response_to_output<O: Terminal>(
        out: std::sync::Arc<O>,
        list_spu_groups: Vec<Metadata<SpuGroupSpec>>,
        output_type: OutputType,
    ) -> Result<()> {
        debug!("groups: {:#?}", list_spu_groups);

        if !list_spu_groups.is_empty() {
            let groups = ListSpuGroups(list_spu_groups);
            out.render_list(&groups, output_type)?;
            Ok(())
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
            Row::from(["NAME", "REPLICAS", "MIN ID", "RACK", "SIZE", "STATUS"])
        }

        /// return errors in string format
        fn errors(&self) -> Vec<String> {
            self.0.iter().map(|_g| "".to_owned()).collect()
        }

        /// table content implementation
        fn content(&self) -> Vec<Row> {
            self.0
                .iter()
                .map(|r| {
                    let spec = &r.spec;
                    let storage_config = spec.spu_config.real_storage_config();
                    Row::from([
                        Cell::new(&r.name).set_alignment(CellAlignment::Right),
                        Cell::new(spec.replicas.to_string()).set_alignment(CellAlignment::Center),
                        Cell::new(r.spec.min_id.to_string()).set_alignment(CellAlignment::Right),
                        Cell::new(spec.spu_config.rack.clone().unwrap_or_default())
                            .set_alignment(CellAlignment::Right),
                        Cell::new(storage_config.size).set_alignment(CellAlignment::Right),
                        Cell::new(r.status.to_string()).set_alignment(CellAlignment::Right),
                    ])
                })
                .collect()
        }
    }
}
