use std::sync::Arc;
use std::time::SystemTime;

use anyhow::Result;
use clap::Parser;
use serde::Serialize;

use fluvio_extension_common::target::ClusterTarget;
use fluvio_extension_common::{OutputFormat, Terminal};
use fluvio_sc_schema::mirror::{MirrorSpec, MirrorType};

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
        let list = admin.all::<MirrorSpec>().await?;
        let now = SystemTime::now().duration_since(SystemTime::UNIX_EPOCH)?;

        let outlist: Vec<HomeStatusRow> = list
            .into_iter()
            .filter_map(|item| {
                match item.spec.mirror_type {
                    MirrorType::Home(home) => {
                        Some(HomeStatusRow {
                            home: home.id.to_string(),                       // Source ID
                            route: home.public_endpoint,                     // Route
                            sc_status: item.status.pairing_sc.to_string(),   // SC Status
                            spu_status: item.status.pairing_spu.to_string(), // SPU Status
                            last_seen: item.status.last_seen(now),           // Last-Seen
                            errors: item.status.pair_errors(),               // Errors
                        })
                    }
                    _ => None,
                }
            })
            .collect();
        output::format(out, outlist, self.output.format)
    }
}

#[derive(Serialize)]
struct HomeStatusRow {
    home: String,
    route: String,
    sc_status: String,
    spu_status: String,
    last_seen: String,
    errors: String,
}

mod output {

    //!
    //! # Fluvio list - output processing
    //!
    use comfy_table::{Cell, Row};
    use comfy_table::CellAlignment;
    use serde::Serialize;
    use anyhow::Result;

    use fluvio_extension_common::output::OutputType;
    use fluvio_extension_common::Terminal;
    use fluvio_extension_common::output::TableOutputHandler;
    use fluvio_extension_common::t_println;

    use super::HomeStatusRow;

    #[derive(Serialize)]
    struct TableList(Vec<HomeStatusRow>);

    // -----------------------------------
    // Format Output
    // -----------------------------------

    pub fn format<O: Terminal>(
        out: std::sync::Arc<O>,
        listvec: Vec<HomeStatusRow>,
        output_type: OutputType,
    ) -> Result<()> {
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
            Row::from(
                [
                    "HOME",
                    "ROUTE",
                    "SC STATUS",
                    "SPU STATUS",
                    "LAST SEEN",
                    "ERRORS",
                ]
                .iter(),
            )
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
                        Cell::new(&e.home).set_alignment(CellAlignment::Left),
                        Cell::new(&e.route).set_alignment(CellAlignment::Left),
                        Cell::new(&e.sc_status).set_alignment(CellAlignment::Left),
                        Cell::new(&e.spu_status).set_alignment(CellAlignment::Left),
                        Cell::new(&e.last_seen).set_alignment(CellAlignment::Left),
                        Cell::new(&e.errors).set_alignment(CellAlignment::Left),
                    ])
                })
                .collect()
        }
    }
}
