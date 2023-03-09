use std::sync::Arc;
use std::fmt::Debug;

use async_trait::async_trait;
use clap::Parser;
use tokio::select;
use anyhow::Result;

use fluvio::metadata::smartmodule::SmartModuleSpec;
use fluvio::Fluvio;
use fluvio_future::io::StreamExt;

use crate::client::cmd::ClientCmd;
use crate::client::smartmodule::list::ListSmartModuleOpt;
use crate::common::output::Terminal;
use crate::common::OutputFormat;

/// Watch for changes to SmartModules
#[derive(Debug, Parser)]
pub struct WatchSmartModuleOpt {
    #[clap(flatten)]
    output: OutputFormat,
}

#[async_trait]
impl ClientCmd for WatchSmartModuleOpt {
    async fn process_client<O: Terminal + Debug + Send + Sync>(
        self,
        out: Arc<O>,
        fluvio: &Fluvio,
    ) -> Result<()> {
        let admin = fluvio.admin().await;

        let mut watch_stream = admin.watch::<SmartModuleSpec>().await?;
        loop {
            select! {
                next = watch_stream.next() => {
                    if let Some(Ok(_event)) = next {
                        out.println("");    // add newline
                        let opt = ListSmartModuleOpt::new(self.output.clone());
                        opt.process_client(out.clone(), fluvio).await?;
                    } else {
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}
