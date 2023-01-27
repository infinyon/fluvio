use std::path::PathBuf;
use std::sync::Arc;
use std::fmt::Debug;

use clap::Parser;
use anyhow::Result;

use fluvio_extension_common::Terminal;
use fluvio_hub_util::HUB_API_CONN_LIST;

use crate::{error::CliError};
use crate::common::OutputFormat;

use super::{get_pkg_list, get_hub_access};

/// List available Connectors in the hub
#[derive(Debug, Parser)]
pub enum ConnectorHubSubCmd {
    /// List all available SmartConnectors
    #[clap(name = "list")]
    List(ConnectorHubListOpts),

    /// Download SmartConnector to the local folder
    #[clap(name = "download")]
    Download(ConnectorHubDownloadOpts),
}

impl ConnectorHubSubCmd {
    pub async fn process<O: Terminal + Debug + Send + Sync>(self, out: Arc<O>) -> Result<()> {
        match self {
            ConnectorHubSubCmd::List(opts) => opts.process(out).await,
            ConnectorHubSubCmd::Download(opts) => opts.process(out).await,
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

#[derive(Debug, Parser)]
pub struct ConnectorHubDownloadOpts {
    /// SmartConnector name: e.g. infinyon/salesforce-sink@v0.0.1
    #[clap(value_name = "name", required = true)]
    package_name: String,

    /// Target local folder or file name
    #[clap(short, long, value_name = "PATH")]
    output: Option<PathBuf>,

    #[clap(long, hide_short_help = true)]
    remote: Option<String>,
}

impl ConnectorHubDownloadOpts {
    pub async fn process<O: Terminal + Debug + Send + Sync>(self, _out: Arc<O>) -> Result<()> {
        let access = get_hub_access(&self.remote)?;

        let package_name = self.package_name;
        let file_name = fluvio_hub_util::cli_pkgname_to_filename(&package_name).map_err(|_| {
            CliError::HubError(format!(
                "invalid package name format {package_name}, is it the form infinyon/json-sql@0.1.0"
            ))
        })?;

        let file_path = if let Some(mut output) = self.output {
            if output.is_dir() {
                output.push(file_name);
            }
            output
        } else {
            PathBuf::from(file_name)
        };
        println!(
            "downloading {package_name} to {}",
            file_path.to_string_lossy()
        );

        let url = fluvio_hub_util::cli_conn_pkgname_to_url(&package_name, &access.remote)
            .map_err(|_| CliError::HubError(format!("invalid pkgname {package_name}")))?;

        let data = fluvio_hub_util::get_package(&url, &access)
            .await
            .map_err(|err| {
                CliError::HubError(format!("downloading {package_name} failed\nServer: {err}"))
            })?;

        std::fs::write(file_path, data).map_err(|err| {
            CliError::Other(format!(
                "unable to write downloaded package to the disk: {err}"
            ))
        })?;
        println!("... downloading complete");
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
    use anyhow::Result;

    use fluvio_extension_common::output::OutputType;
    use fluvio_extension_common::Terminal;
    use fluvio_extension_common::output::TableOutputHandler;
    use fluvio_extension_common::t_println;
    use fluvio_hub_util::PackageMeta;

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
