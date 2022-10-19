use std::sync::Arc;
use std::fmt::Debug;

use clap::Parser;

use fluvio_extension_common::Terminal;
use fluvio_hub_util as hubutil;
use hubutil::PackageList;
use hubutil::http;

use crate::{CliError, Result};
use crate::common::OutputFormat;

const API_LIST: &str = "hub/v0/list";

/// List available SmartModules in the hub
#[derive(Debug, Parser)]
pub struct ListHubOpt {
    #[clap(flatten)]
    output: OutputFormat,

    #[clap(long, hide_short_help = true)]
    remote: Option<String>,
}

impl ListHubOpt {
    pub async fn process<O: Terminal + Debug + Send + Sync>(self, out: Arc<O>) -> Result<()> {
        let access = hubutil::HubAccess::default_load(&self.remote).map_err(|_| {
            CliError::HubError("missing access credentials, try 'fluvio cloud login'".into())
        })?;
        let action_token = access.get_list_token().await.map_err(|_| {
            CliError::HubError("missing access credentials, try 'fluvio cloud login'".into())
        })?;
        let url = format!("{}/{API_LIST}", &access.remote);
        let mut res = http::get(&url)
            .header("Authorization", &action_token)
            .await
            .map_err(|e| CliError::HubError(format!("list api access error {e}")))?;
        let pl: PackageList = res
            .body_json()
            .await
            .map_err(|e| CliError::HubError(format!("list api data parse error {e}")))?;
        output::smartmodules_response_to_output(out, pl.packages, self.output.format)?;
        Ok(())
    }
}

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

    use crate::CliError;
    use fluvio_extension_common::output::TableOutputHandler;
    use fluvio_extension_common::t_println;

    #[derive(Serialize)]
    struct ListSmartModules(Vec<String>);

    // -----------------------------------
    // Format Output
    // -----------------------------------

    /// Format SmartModules based on output type
    pub fn smartmodules_response_to_output<O: Terminal>(
        out: std::sync::Arc<O>,
        list_smartmodules: Vec<String>,
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
            Row::from(["SMARTMODULE"])
        }

        /// return errors in string format
        fn errors(&self) -> Vec<String> {
            vec![]
        }

        /// table content implementation                     
        fn content(&self) -> Vec<Row> {
            self.0
                .iter()
                .map(|e| Row::from([Cell::new(&e).set_alignment(CellAlignment::Left)]))
                .collect()
        }
    }
}
