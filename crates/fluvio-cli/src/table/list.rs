//! # List Tables CLI
//!
//! CLI tree and processing to list Tables
//!

use std::sync::Arc;
use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio_controlplane_metadata::table::TableSpec;

use fluvio_extension_common::Terminal;
use fluvio_extension_common::OutputFormat;
use crate::CliError;

#[derive(Debug, StructOpt)]
pub struct ListTablesOpt {
    #[structopt(flatten)]
    output: OutputFormat,
}

impl ListTablesOpt {
    /// Process list connectors cli request
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<(), CliError> {
        let admin = fluvio.admin().await;
        let lists = admin.list::<TableSpec, _>(vec![]).await?;

        output::tables_response_to_output(out, lists, self.output.format)
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
    use fluvio_controlplane_metadata::table::TableSpec;

    use crate::CliError;
    use fluvio_extension_common::output::TableOutputHandler;
    use fluvio_extension_common::t_println;

    #[derive(Serialize)]
    struct ListTables(Vec<Metadata<TableSpec>>);

    // -----------------------------------
    // Format Output
    // -----------------------------------

    /// Format Table based on output type
    pub fn tables_response_to_output<O: Terminal>(
        out: std::sync::Arc<O>,
        list_tables: Vec<Metadata<TableSpec>>,
        output_type: OutputType,
    ) -> Result<(), CliError> {
        debug!("tables: {:#?}", list_tables);

        if !list_tables.is_empty() {
            let connectors = ListTables(list_tables);
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
    impl TableOutputHandler for ListTables {
        /// table header implementation
        fn header(&self) -> Row {
            row!["NAME", "STATUS",]
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
