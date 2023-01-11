//! # List TableFormats CLI
//!
//! CLI tree and processing to list TableFormats
//!

use std::sync::Arc;

use clap::Parser;
use anyhow::Result;

use fluvio::Fluvio;
use fluvio::metadata::tableformat::TableFormatSpec;

use fluvio_extension_common::Terminal;
use fluvio_extension_common::OutputFormat;

#[derive(Debug, Parser)]
pub struct ListTableFormatsOpt {
    #[clap(flatten)]
    output: OutputFormat,
}

impl ListTableFormatsOpt {
    /// Process list connectors cli request
    pub async fn process<O: Terminal>(self, out: Arc<O>, fluvio: &Fluvio) -> Result<()> {
        let admin = fluvio.admin().await;
        let lists = admin.all::<TableFormatSpec>().await?;

        output::tableformats_response_to_output(out, lists, self.output.format)
    }
}

mod output {

    //!
    //! # Fluvio SC - output processing
    //!

    use comfy_table::{Row, Cell};
    use comfy_table::CellAlignment;
    use tracing::debug;
    use serde::Serialize;
    use anyhow::Result;

    use fluvio_extension_common::output::OutputType;
    use fluvio_extension_common::Terminal;
    use fluvio::metadata::objects::Metadata;
    use fluvio::metadata::tableformat::TableFormatSpec;
    use fluvio_extension_common::output::TableOutputHandler;
    use fluvio_extension_common::t_println;

    #[derive(Serialize)]
    struct ListTableFormats(Vec<Metadata<TableFormatSpec>>);

    // -----------------------------------
    // Format Output
    // -----------------------------------

    /// Format TableFormat list
    pub fn tableformats_response_to_output<O: Terminal>(
        out: std::sync::Arc<O>,
        list_tableformats: Vec<Metadata<TableFormatSpec>>,
        output_type: OutputType,
    ) -> Result<()> {
        debug!("tableformats: {:#?}", list_tableformats);

        if !list_tableformats.is_empty() {
            let tableformats = ListTableFormats(list_tableformats);
            out.render_list(&tableformats, output_type)?;
            Ok(())
        } else {
            t_println!(out, "no tableformats");
            Ok(())
        }
    }

    // -----------------------------------
    // Output Handlers
    // -----------------------------------
    impl TableOutputHandler for ListTableFormats {
        /// tableformat header implementation
        fn header(&self) -> Row {
            Row::from(["NAME", "STATUS"])
        }

        /// return errors in string format
        fn errors(&self) -> Vec<String> {
            vec![]
        }

        /// table content implementation for tableformat (sry, naming makes this confusing)
        fn content(&self) -> Vec<Row> {
            self.0
                .iter()
                .map(|r| {
                    let _spec = &r.spec;

                    Row::from([
                        Cell::new(&r.name).set_alignment(CellAlignment::Right),
                        Cell::new(r.status.to_string()).set_alignment(CellAlignment::Right),
                    ])
                })
                .collect()
        }
    }
}
