// use std::sync::Arc;

// use anyhow::Result;
// use clap::Parser;

// mod export;

// use export::ExportOpt;
// use fluvio_extension_common::{target::ClusterTarget, Terminal};

// #[derive(Debug, Parser)]
// pub enum ExportOpt {
//     // export metadata for remote cluster
//     #[command(name = "export")]
//     Export(ExportOpt),
// }

// impl ExportOpt {
//     pub async fn execute<T: Terminal>(
//         self,
//         out: Arc<T>,
//         cluster_target: ClusterTarget,
//     ) -> Result<()> {
//         match self {
//             ExportOpt::Export(export) => export.execute(out, cluster_target).await,
//         }
//     }
// }
