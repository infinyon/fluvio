pub use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Result;
use clap::Parser;
use fluvio_extension_common::target::ClusterTarget;
use fluvio_extension_common::{OutputFormat, Terminal};
use fluvio_sc_schema::remote::{RemoteSpec, RemoteType};

use super::get_admin;

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
        let list = admin.all::<RemoteSpec>().await?;
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;

        let outlist: Vec<(String, String, String, String)> = list
            .into_iter()
            .filter_map(|item| {
                match item.spec.remote_type {
                    RemoteType::Core(core) => {
                        Some((
                            core.id.to_string(),        // Source ID
                            core.public_endpoint,       // Route
                            item.status.to_string(),    // Status
                            item.status.last_seen(now), // Last-Seen
                        ))
                    }
                    _ => None,
                }
            })
            .collect();
        output::format(out, outlist, self.output.format)
    }
}

mod output {

    //!
    //! # Fluvio list - output processing
    //!
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
            Row::from(["REMOTE", "ROUTE", "STATUS", "LAST SEEN"])
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
