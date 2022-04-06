//! # List Managed Connectors CLI
//!
//! CLI tree and processing to list Managed Connectors
//!

use std::sync::Arc;
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio::metadata::connector::ManagedConnectorSpec;

use fluvio_extension_common::Terminal;
use fluvio_extension_common::OutputFormat;
use crate::CliError;

#[derive(Debug, StructOpt)]
pub struct ListManagedConnectorsOpt {
    #[structopt(flatten)]
    output: OutputFormat,
}

impl ListManagedConnectorsOpt {
    /// Process list connectors cli request
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<(), CliError> {
        let admin = fluvio.admin().await;
        let lists = admin.list::<ManagedConnectorSpec, _>(vec![]).await?;

        output::managed_connectors_response_to_output(out, lists, self.output.format)
    }
}

mod output {

    //!
    //! # Fluvio SC - output processing
    //!
    //! Format Managed Connectors response based on output type

    use comfy_table::{Row, Cell};
    use comfy_table::CellAlignment;
    use tracing::debug;
    use serde::Serialize;
    use fluvio_extension_common::output::OutputType;
    use fluvio_extension_common::Terminal;

    use fluvio::metadata::objects::Metadata;
    use fluvio::metadata::connector::ManagedConnectorSpec;

    use crate::CliError;
    use fluvio_extension_common::output::TableOutputHandler;
    use fluvio_extension_common::t_println;

    #[derive(Serialize)]
    struct ListManagedConnectors(Vec<Metadata<ManagedConnectorSpec>>);

    // -----------------------------------
    // Format Output
    // -----------------------------------

    /// Format Managed Connectors based on output type
    pub fn managed_connectors_response_to_output<O: Terminal>(
        out: std::sync::Arc<O>,
        list_managed_connectors: Vec<Metadata<ManagedConnectorSpec>>,
        output_type: OutputType,
    ) -> Result<(), CliError> {
        debug!("managed connectors: {:#?}", list_managed_connectors);

        if !list_managed_connectors.is_empty() {
            let connectors = ListManagedConnectors(list_managed_connectors);
            out.render_list(&connectors, output_type)?;
            Ok(())
        } else {
            t_println!(out, "no managed connectors");
            Ok(())
        }
    }

    // -----------------------------------
    // Output Handlers
    // -----------------------------------
    impl TableOutputHandler for ListManagedConnectors {
        /// table header implementation
        fn header(&self) -> Row {
            row!["NAME", "TYPE", "VERSION", "STATUS",]
        }

        /// return errors in string format
        fn errors(&self) -> Vec<String> {
            vec![]
        }

        /// table content implementation
        fn content(&self) -> Vec<Row> {
            self.0
                .iter()
                .map(|r| {
                    let spec = &r.spec;
                    let mut row = Row::new();
                    row.add_cell(Cell::new_align(&r.name, Alignment::LEFT));
                    row.add_cell(Cell::new_align(&spec.type_.to_string(), Alignment::LEFT));
                    row.add_cell(Cell::new_align(&spec.version(), Alignment::LEFT));
                    row.add_cell(Cell::new_align(&r.status.to_string(), Alignment::RIGHT));
                })
                .collect()
        }
    }
}
