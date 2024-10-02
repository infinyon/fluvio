use std::sync::Arc;
use std::fmt::Debug;

use chrono::DateTime;
use chrono::Utc;
use clap::Parser;
use anyhow::Result;

use fluvio_extension_common::Terminal;
use fluvio_extension_common::OutputFormat;
use crate::HUB_API_CONN_LIST;

use super::get_pkg_list;

/// List all available SmartConnectors
#[derive(Debug, Parser)]
pub struct ConnectorHubListOpts {
    #[clap(flatten)]
    output: OutputFormat,

    #[arg(long, hide = true)]
    system: bool,

    /// Show exact time instead of relative time for `Release` column
    #[arg(long, default_value = "false")]
    exact_time: bool,

    #[arg(long, hide_short_help = true)]
    remote: Option<String>,
}

impl ConnectorHubListOpts {
    pub async fn process<O: Terminal + Debug + Send + Sync>(self, out: Arc<O>) -> Result<()> {
        let pl = get_pkg_list(HUB_API_CONN_LIST, &self.remote, self.system).await?;
        output::tableformat(out, pl.packages, self.output.format, self.exact_time)?;
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
    use fluvio_extension_common::Terminal;
    use fluvio_extension_common::output::TableOutputHandler;
    use fluvio_extension_common::t_println;

    use crate::{PackageMeta, PackageMetaExt};

    use super::format_duration;

    #[derive(Serialize)]
    struct ListConnectors {
        pkgs: Vec<PackageMeta>,
        exact_time: bool,
    }

    // -----------------------------------
    // Format Output
    // -----------------------------------

    /// Format SmartModules based on output type
    pub fn tableformat<O: Terminal>(
        out: std::sync::Arc<O>,
        list_pkgs: Vec<PackageMeta>,
        output_type: OutputType,
        exact_time: bool,
    ) -> Result<()> {
        debug!("connectors: {:#?}", list_pkgs);

        if !list_pkgs.is_empty() {
            let connectors = ListConnectors {
                pkgs: list_pkgs,
                exact_time,
            };
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
            Row::from(["Connector", "Visibility", "Released"])
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

fn format_duration(date: DateTime<Utc>, exact: bool) -> String {
    let duration = Utc::now().signed_duration_since(date);

    if exact {
        return date.format("%Y-%m-%d %H:%M:%S %Z").to_string();
    }

    if duration.num_weeks() >= 1 {
        return date.format("%Y/%m/%d").to_string();
    }

    if duration.num_days() >= 2 {
        return format!("{} days ago", duration.num_days());
    }

    if duration.num_days() == 1 {
        return String::from("Yesterday");
    }

    if duration.num_hours() >= 1 {
        return format!("{} hours ago", duration.num_hours());
    }

    if duration.num_minutes() >= 1 {
        return format!("{} minutes ago", duration.num_minutes());
    }

    if duration.num_minutes() < 1 {
        return String::from("Just now");
    }

    date.format("%Y/%m/%d").to_string()
}
