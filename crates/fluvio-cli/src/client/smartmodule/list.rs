use std::sync::Arc;
use std::fmt::Debug;

use async_trait::async_trait;
use clap::Parser;
use anyhow::Result;

use fluvio::metadata::smartmodule::SmartModuleSpec;
use fluvio::Fluvio;

use crate::client::cmd::ClientCmd;
use crate::common::output::Terminal;
use crate::common::OutputFormat;

/// List all existing SmartModules
#[derive(Debug, Parser)]
pub struct ListSmartModuleOpt {
    #[clap(flatten)]
    output: OutputFormat,

    #[clap(long)]
    filter: Option<String>,
}

impl ListSmartModuleOpt {
    pub fn new(output: OutputFormat) -> Self {
        Self {
            output,
            filter: None,
        }
    }
}

#[async_trait]
impl ClientCmd for ListSmartModuleOpt {
    async fn process_client<O: Terminal + Debug + Send + Sync>(
        self,
        out: Arc<O>,
        fluvio: &Fluvio,
    ) -> Result<()> {
        let admin = fluvio.admin().await;
        let filters = if let Some(filter) = self.filter {
            vec![filter]
        } else {
            vec![]
        };
        let lists = admin
            .list_with_params::<SmartModuleSpec, _>(filters, true)
            .await?;
        output::smartmodules_response_to_output(out, lists, self.output.format)
    }
}
mod output {

    //!
    //! # Fluvio SC - output processing
    //!
    //! Format SmartModules response based on output type

    use comfy_table::{Cell, Row};
    use comfy_table::CellAlignment;
    use tracing::debug;
    use serde::Serialize;
    use anyhow::Result;

    use fluvio_extension_common::output::OutputType;
    use fluvio_extension_common::Terminal;

    use fluvio::metadata::objects::Metadata;
    use fluvio::metadata::smartmodule::SmartModuleSpec;

    use fluvio_extension_common::output::TableOutputHandler;
    use fluvio_extension_common::t_println;

    #[derive(Serialize)]
    struct ListSmartModules(Vec<Metadata<SmartModuleSpec>>);

    // -----------------------------------
    // Format Output
    // -----------------------------------

    /// Format SmartModules based on output type
    pub fn smartmodules_response_to_output<O: Terminal>(
        out: std::sync::Arc<O>,
        list_smartmodules: Vec<Metadata<SmartModuleSpec>>,
        output_type: OutputType,
    ) -> Result<()> {
        debug!("smart modules: {:#?}", list_smartmodules);

        if !list_smartmodules.is_empty() {
            let smartmodules = ListSmartModules(list_smartmodules);
            out.render_list(&smartmodules, output_type)?;
            Ok(())
        } else {
            t_println!(out, "no smartmodules");
            Ok(())
        }
    }

    // -----------------------------------
    // Output Handlers
    // -----------------------------------
    impl TableOutputHandler for ListSmartModules {
        /// table header implementation
        fn header(&self) -> Row {
            Row::from(["SMARTMODULE", "SIZE"])
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

                    Row::from([
                        Cell::new(r.spec.fqdn(&r.name)).set_alignment(CellAlignment::Left),
                        Cell::new(
                            bytesize::ByteSize::b(
                                r.spec.summary.clone().unwrap_or_default().wasm_length as u64,
                            )
                            .to_string(),
                        )
                        .set_alignment(CellAlignment::Right),
                    ])
                })
                .collect()
        }
    }
}
