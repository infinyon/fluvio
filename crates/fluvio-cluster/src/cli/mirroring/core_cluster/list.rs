use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tracing::info;

use cloud_sc_extra::remote::RemoteList;

use crate::cli::common::OutputFormat;

use super::common::*;

#[derive(Debug, Parser)]
pub struct ListOpt {
    #[clap(flatten)]
    output: OutputFormat,
}
impl ListOpt {
    pub async fn execute<T: Terminal>(
        self,
        out: Arc<T>,
        cluster_target: ClusterTarget,
    ) -> Result<()> {
        let req = RemoteList {};
        info!(req=?req, "remote-cluster list request");
        let resp = send_request(cluster_target, req).await?;
        info!("remote cluster register resp: {}", resp.name);

        let outlist = if let Some(list) = resp.list {
            list.iter()
                .map(|item| {
                    (
                        item.name.clone(),
                        item.remote_type.clone(),
                        item.pairing.clone(),
                        item.status.clone(),
                        item.last.clone(),
                    )
                })
                .collect()
        } else {
            Vec::new()
        };
        output::format(out, outlist, self.output.format)
    }
}

#[allow(dead_code)]
mod output {

    //!
    //! # Fluvio list - output processing
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

    type ListVec = Vec<(String, String, String, String, String)>;

    #[derive(Serialize)]
    struct TableList(ListVec);

    // -----------------------------------
    // Format Output
    // -----------------------------------

    /// Format SmartModules based on output type
    pub fn format<O: Terminal>(
        out: std::sync::Arc<O>,
        listvec: ListVec,
        output_type: OutputType,
    ) -> Result<()> {
        debug!("listvec: {:#?}", listvec);

        if !listvec.is_empty() {
            let rlist = TableList(listvec);
            out.render_list(&rlist, output_type)?;
            Ok(())
        } else {
            t_println!(out, "no items");
            Ok(())
        }
    }

    // -----------------------------------
    // Output Handlers
    // -----------------------------------
    impl TableOutputHandler for TableList {
        /// table header implementation
        fn header(&self) -> Row {
            Row::from([
                "RemoteCluster",
                "RemoteType",
                "Paired",
                "Status",
                "Last Seen",
            ])
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
                        Cell::new(&e.0).set_alignment(CellAlignment::Left),
                        Cell::new(&e.1).set_alignment(CellAlignment::Left),
                        Cell::new(&e.2).set_alignment(CellAlignment::Left),
                        Cell::new(&e.3).set_alignment(CellAlignment::Left),
                        Cell::new(&e.4).set_alignment(CellAlignment::Left),
                    ])
                })
                .collect()
        }
    }
}
