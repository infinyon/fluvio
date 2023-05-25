// schema/list.rs
use std::sync::Arc;
use std::fmt::Debug;

use anyhow::Result;
use clap::Parser;
use serde::Serialize;

use fluvio_extension_common::Terminal;

use crate::common::OutputFormat;

/// List scheamas in the cluster
#[derive(Debug, Parser)]
pub struct ListSchemaOpt {
    #[clap(flatten)]
    output: OutputFormat,
}

impl ListSchemaOpt {
    pub async fn process<O: Terminal + Debug + Send + Sync>(self, out: Arc<O>) -> Result<()> {
        let schemas = vec![SchemaListEntry {
            name: String::from("a_schema"),
            version: String::from("0.1.0"),
        }];
        output::schemas_response_to_output(out, schemas, self.output.format)?;
        Ok(())
    }
}

// placeholder
#[derive(Debug, Serialize)]
pub struct SchemaListEntry {
    pub name: String,
    pub version: String,
}

#[allow(dead_code)]
mod output {
    //!
    //! # Fluvio hub list - output processing
    //!
    //! Format SmartModules response based on output type
    use comfy_table::{Cell, Row};
    use comfy_table::CellAlignment;
    use tracing::debug;
    use serde::Serialize;
    use anyhow::Result;

    use fluvio_extension_common::output::OutputType;
    use fluvio_extension_common::Terminal;
    use fluvio_extension_common::output::TableOutputHandler;
    use fluvio_extension_common::t_println;

    use super::SchemaListEntry;

    #[derive(Debug, Serialize)]
    struct ListSchemas(Vec<SchemaListEntry>);

    // -----------------------------------
    // Format Output
    // -----------------------------------

    /// Format Schemas based on output type
    pub fn schemas_response_to_output<O: Terminal>(
        out: std::sync::Arc<O>,
        list_schemas: Vec<SchemaListEntry>,
        output_type: OutputType,
    ) -> Result<()> {
        debug!("schemas: {:#?}", list_schemas);

        if list_schemas.is_empty() {
            t_println!(out, "no smartmodules");
            Ok(())
        } else {
            let schemas = ListSchemas(list_schemas);
            out.render_list(&schemas, output_type)?;
            Ok(())
        }
    }

    // -----------------------------------
    // Output Handlers
    // -----------------------------------
    impl TableOutputHandler for ListSchemas {
        /// table header implementation
        fn header(&self) -> Row {
            Row::from(["SCHEMA", "VERSION"])
        }

        /// return errors in string format
        fn errors(&self) -> Vec<String> {
            vec![]
        }

        /// table content implementation
        fn content(&self) -> Vec<Row> {
            self.0
                .iter()
                .map(|e| {
                    Row::from([
                        Cell::new(&e.name).set_alignment(CellAlignment::Left),
                        Cell::new(&e.version).set_alignment(CellAlignment::Left),
                    ])
                })
                .collect()
        }
    }
}
