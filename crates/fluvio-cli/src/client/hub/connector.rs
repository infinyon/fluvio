use std::sync::Arc;
use std::fmt::Debug;

use clap::Parser;

use fluvio_extension_common::Terminal;
use fluvio_hub_util::HUB_API_CONN_LIST;

use crate::Result;
use crate::common::OutputFormat;

use super::get_pkg_list;

/// List available Connectors in the hub
#[derive(Debug, Parser)]
pub enum ConnectorHubSubCmd {
    #[clap(name = "list")]
    List(ConnectorHubListOpts),
}

impl ConnectorHubSubCmd {
    pub async fn process<O: Terminal + Debug + Send + Sync>(self, out: Arc<O>) -> Result<()> {
        match self {
            ConnectorHubSubCmd::List(opts) => opts.process(out).await,
        }
    }
}

#[derive(Debug, Parser)]
pub struct ConnectorHubListOpts {
    #[clap(flatten)]
    output: OutputFormat,

    #[clap(long, hide_short_help = true)]
    remote: Option<String>,
}

impl ConnectorHubListOpts {
    pub async fn process<O: Terminal + Debug + Send + Sync>(self, out: Arc<O>) -> Result<()> {
        let pl = get_pkg_list(HUB_API_CONN_LIST, &self.remote).await?;
        output::tableformat(out, pl.packages, self.output.format)?;
        Ok(())
    }
}

// #[allow(dead_code)]
mod output {

    //!
    //! # Fluvio hub list - output processing
    //!
    //! Format SmartModules response based on output type
    use comfy_table::{Cell, Row};
    use comfy_table::CellAlignment;

    use tracing::debug;
    use serde::Serialize;
    use fluvio_extension_common::output::OutputType;
    use fluvio_extension_common::Terminal;
    use fluvio_extension_common::output::TableOutputHandler;
    use fluvio_extension_common::t_println;
    use fluvio_hub_util::PackageMeta;

    use crate::CliError;

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
    ) -> Result<(), CliError> {
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
            Row::from(["CONNECTOR", "Visibility"])
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
                    ])
                })
                .collect()
        }
    }
}
