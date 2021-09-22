//! # List Managed Connectors CLI
//!
//! CLI tree and processing to list Managed Connectors
//!

use std::sync::Arc;
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio_controlplane_metadata::connector::ManagedConnectorSpec;

use fluvio_extension_common::Terminal;
use fluvio_extension_common::OutputFormat;
use crate::error::ConnectorError;

#[derive(Debug, StructOpt)]
pub struct ListManagedConnectorsOpt {
    #[structopt(flatten)]
    output: OutputFormat,
}

impl ListManagedConnectorsOpt {
    /// Process list connectors cli request
    pub async fn process<O: Terminal>(
        self,
        out: Arc<O>,
        fluvio: &Fluvio,
    ) -> Result<(), ConnectorError> {
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

    use prettytable::Row;
    use prettytable::row;
    use prettytable::Cell;
    use prettytable::cell;
    use prettytable::format::Alignment;
    use tracing::debug;
    use serde::Serialize;
    use fluvio_extension_common::output::OutputType;
    use fluvio_extension_common::Terminal;

    use fluvio::metadata::objects::Metadata;
    use fluvio_controlplane_metadata::connector::ManagedConnectorSpec;

    use crate::error::ConnectorError;
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
    ) -> Result<(), ConnectorError> {
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
            row!["NAME", "STATUS",]
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
                    let _spec = &r.spec;
                    Row::new(vec![
                        Cell::new_align(&r.name, Alignment::RIGHT),
                        Cell::new_align(&r.status.to_string(), Alignment::RIGHT),
                    ])
                })
                .collect()
        }
    }
}
