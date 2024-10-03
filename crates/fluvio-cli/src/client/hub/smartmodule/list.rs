use std::sync::Arc;
use std::fmt::Debug;

use clap::Parser;
use anyhow::Result;

use fluvio_extension_common::Terminal;
use fluvio_hub_util::HUB_API_LIST_META;
use fluvio_hub_util::cmd::get_pkg_list;

use crate::common::OutputFormat;

/// List available SmartModules in the hub
#[derive(Debug, Parser)]
pub struct SmartModuleHubListOpts {
    #[clap(flatten)]
    output: OutputFormat,

    /// Show exact time instead of relative time for `Released` column
    #[arg(long, default_value = "false")]
    exact_time: bool,

    #[arg(long, hide = true)]
    system: bool,

    #[arg(long, hide_short_help = true)]
    remote: Option<String>,
}

impl SmartModuleHubListOpts {
    pub async fn process<O: Terminal + Debug + Send + Sync>(self, out: Arc<O>) -> Result<()> {
        let pl = get_pkg_list(HUB_API_LIST_META, &self.remote, self.system).await?;
        output::smartmodules_response_to_output(
            out,
            pl.packages,
            self.output.format,
            self.exact_time,
        )?;
        Ok(())
    }
}

#[allow(dead_code)]
mod output {

    //!
    //! # Fluvio hub list - output processing
    //!
    //! Format SmartModules response based on output type
    use comfy_table::{Cell, Row};
    use comfy_table::CellAlignment;
    use fluvio_extension_common::time::format_duration;
    use tracing::debug;
    use serde::Serialize;
    use anyhow::Result;

    use fluvio_extension_common::output::OutputType;
    use fluvio_extension_common::Terminal;
    use fluvio_extension_common::output::TableOutputHandler;
    use fluvio_extension_common::t_println;
    use fluvio_hub_util::{PackageMeta, PackageMetaExt};

    #[derive(Serialize)]
    struct ListSmartModules {
        pkgs: Vec<PackageMeta>,
        exact_time: bool,
    }

    // -----------------------------------
    // Format Output
    // -----------------------------------

    /// Format SmartModules based on output type
    pub fn smartmodules_response_to_output<O: Terminal>(
        out: std::sync::Arc<O>,
        list_smartmodules: Vec<PackageMeta>,
        output_type: OutputType,
        exact_time: bool,
    ) -> Result<()> {
        debug!("smartmodules: {:#?}", list_smartmodules);

        if !list_smartmodules.is_empty() {
            let smartmodules = ListSmartModules {
                pkgs: list_smartmodules,
                exact_time,
            };
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
            Row::from(["SmartModule", "Visibility", "Released"])
        }

        /// return errors in string format
        fn errors(&self) -> Vec<String> {
            vec![]
        }

        /// table content implementation
        fn content(&self) -> Vec<Row> {
            self.pkgs
                .iter()
                .map(|e| {
                    Row::from([
                        Cell::new(e.pkg_name()).set_alignment(CellAlignment::Left),
                        Cell::new(&e.visibility).set_alignment(CellAlignment::Left),
                        Cell::new(
                            e.published_at()
                                .map(|date| format_duration(date, self.exact_time))
                                .unwrap_or(String::from("N/A")),
                        ),
                    ])
                })
                .collect()
        }
    }
}
