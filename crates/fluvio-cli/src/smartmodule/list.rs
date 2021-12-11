use std::sync::Arc;

use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;

use crate::common::output::Terminal;
use crate::common::OutputFormat;
use crate::Result;
/// List all existing SmartModules
#[derive(Debug, StructOpt)]
pub struct ListSmartModuleOpt {
    #[structopt(flatten)]
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
    use fluvio::metadata::smartmodule::SmartModuleMetadataSpec;

    use crate::CliError;
    use fluvio_extension_common::output::TableOutputHandler;
    use fluvio_extension_common::t_println;

    #[derive(Serialize)]
    struct ListSmartModules(Vec<Metadata<SmartModuleMetadataSpec>>);

    // -----------------------------------
    // Format Output
    // -----------------------------------

    /// Format Smart Modules based on output type
    pub fn smartmodules_response_to_output<O: Terminal>(
        out: std::sync::Arc<O>,
        list_smartmodules: Vec<Metadata<SmartModuleMetadataSpec>>,
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
            row!["NAME", "STATUS", "SIZE"]
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
                        Cell::new_align(&r.spec.wasm_size.to_string(), Alignment::RIGHT),
                    ])
                })
                .collect()
        }
    }
}
