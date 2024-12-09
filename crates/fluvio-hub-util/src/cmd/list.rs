use std::sync::Arc;
use std::fmt::Debug;

use clap::Parser;
use anyhow::Result;

use fluvio_extension_common::Terminal;
use fluvio_extension_common::OutputFormat;
use crate::HUB_API_CONN_LIST;

use super::get_pkg_list;

/// List all available Connectors
#[derive(Debug, Parser)]
pub struct ConnectorHubListOpts {
    #[clap(flatten)]
    output: OutputFormat,

    #[arg(long, hide = true)]
    system: bool,

    #[arg(long, hide_short_help = true)]
    remote: Option<String>,
}

impl ConnectorHubListOpts {
    pub async fn process<O: Terminal + Debug + Send + Sync>(self, out: Arc<O>) -> Result<()> {
        let pl = get_pkg_list(HUB_API_CONN_LIST, &self.remote, self.system).await?;
        output::tableformat(out, pl.packages, self.output.format)?;
        Ok(())
    }
}

mod output {
    //! # Fluvio hub list - output processing
    //!
    //! Format SmartModules response based on output type
    use comfy_table::{Cell, Row};
    use comfy_table::CellAlignment;
    use tracing::debug;
    use serde::Serialize;
    use anyhow::Result;

    use fluvio_extension_common::output::OutputType;
    use fluvio_extension_common::time::time_elapsed;
    use fluvio_extension_common::Terminal;
    use fluvio_extension_common::output::TableOutputHandler;
    use fluvio_extension_common::t_println;

    use crate::{PackageMeta, PackageMetaExt};

    #[derive(Serialize)]
    struct ListConnectors(Vec<PackageMeta>);

    // -----------------------------------
    // Format Output
    // -----------------------------------

    /// Format SmartModules based on output type
    pub fn tableformat<O: Terminal>(
        out: std::sync::Arc<O>,
        list_pkgs: Vec<PackageMeta>,
        output_type: OutputType,
    ) -> Result<()> {
        debug!("connectors: {:#?}", list_pkgs);

        if !list_pkgs.is_empty() {
            let connectors = ListConnectors(list_pkgs);
            out.render_list(&connectors, output_type)?;
            Ok(())
        } else {
            t_println!(out, "no connectors");
            Ok(())
        }
    }

    // -----------------------------------
    // Output Handlers
    // -----------------------------------
    impl TableOutputHandler for ListConnectors {
        /// table header implementation
        fn header(&self) -> Row {
            Row::from(["CONNECTOR", "Visibility", "Released"])
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
                        Cell::new(e.pkg_name()).set_alignment(CellAlignment::Left),
                        Cell::new(&e.visibility).set_alignment(CellAlignment::Left),
                        Cell::new(
                            e.published_at()
                                .map(|date| time_elapsed(date).unwrap_or(String::from("N/A")))
                                .unwrap_or(String::from("N/A")),
                        ),
                    ])
                })
                .collect()
        }
    }
}
