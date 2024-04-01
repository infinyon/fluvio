use std::sync::Arc;
use clap::Parser;
use fluvio_controlplane_metadata::upstream_cluster::{UpstreamClusterSpec, UpstreamClusterStatus};
use fluvio_extension_common::{target::ClusterTarget, OutputFormat, Terminal};
use anyhow::Result;

use crate::cli::mirroring::core_cluster::get_admin;

#[derive(Debug, Parser)]
pub struct StatusOpt {
    #[clap(flatten)]
    output: OutputFormat,
}

impl StatusOpt {
    pub async fn execute<T: Terminal>(
        self,
        out: Arc<T>,
        cluster_target: ClusterTarget,
    ) -> Result<()> {
        let admin = get_admin(cluster_target).await?;
        let list = admin.all::<UpstreamClusterSpec>().await?;

        let outlist: Vec<(String, String, String, String)> = list
            .iter()
            .map(|item| {
                let status: UpstreamClusterStatus = item.status.clone();
                (
                    item.spec.source_id.to_string(),              // Source ID
                    item.spec.target.endpoint.to_string(),        // Route TODO: print tls
                    status.to_string(),                           // Status
                    status.connection_stat.last_sent.to_string(), // Last-Sent
                )
            })
            .collect();
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

    type ListVec = Vec<(String, String, String, String)>;

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
            Row::from(["Remote Cluster", "Route", "Status", "Last-Sent"])
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
                    ])
                })
                .collect()
        }
    }
}
