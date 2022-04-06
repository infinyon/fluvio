use std::sync::Arc;
use clap::Parser;
use crate::Result;
use fluvio::metadata::smartmodule::SmartModuleSpec;
use fluvio::Fluvio;
use crate::common::output::Terminal;
use crate::common::OutputFormat;

/// List all existing SmartModules
#[derive(Debug, Parser)]
pub struct ListSmartModuleOpt {
    #[clap(flatten)]
    output: OutputFormat,
}

impl ListSmartModuleOpt {
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        let admin = fluvio.admin().await;
        let lists = admin.list::<SmartModuleSpec, _>(vec![]).await?;
        output::smartmodules_response_to_output(out, lists, self.output.format)
    }
}
mod output {

    //!
    //! # Fluvio SC - output processing
    //!
    //! Format Smart Modules response based on output type

    use comfy_table::{Cell, Row};
    use comfy_table::CellAlignment;

    use tracing::debug;
    use serde::Serialize;
    use fluvio_extension_common::output::OutputType;
    use fluvio_extension_common::Terminal;

    use fluvio::metadata::objects::Metadata;
    use fluvio::metadata::smartmodule::SmartModuleSpec;

    use crate::CliError;
    use fluvio_extension_common::output::TableOutputHandler;
    use fluvio_extension_common::t_println;

    #[derive(Serialize)]
    struct ListSmartModules(Vec<Metadata<SmartModuleSpec>>);

    // -----------------------------------
    // Format Output
    // -----------------------------------

    /// Format Smart Modules based on output type
    pub fn smartmodules_response_to_output<O: Terminal>(
        out: std::sync::Arc<O>,
        list_smartmodules: Vec<Metadata<SmartModuleSpec>>,
        output_type: OutputType,
    ) -> Result<(), CliError> {
        debug!("smart modules: {:#?}", list_smartmodules);

        if !list_smartmodules.is_empty() {
            let smartmodules = ListSmartModules(list_smartmodules);
            out.render_list(&smartmodules, output_type)?;
            Ok(())
        } else {
            t_println!(out, "no smart modules");
            Ok(())
        }
    }

    // -----------------------------------
    // Output Handlers
    // -----------------------------------
    impl TableOutputHandler for ListSmartModules {
        /// table header implementation
        fn header(&self) -> Row {
            Row::from(["NAME", "STATUS", "SIZE"])
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
                    let mut row = Row::new();

                    row.add_cell(Cell::new(&r.name).set_alignment(CellAlignment::Right));

                    row.add_cell(
                        Cell::new(&r.status.to_string()).set_alignment(CellAlignment::Right),
                    );

                    row.add_cell(
                        Cell::new(&r.spec.wasm.payload.len().to_string())
                            .set_alignment(CellAlignment::Right),
                    );

                    row
                })
                .collect()
        }
    }
}
