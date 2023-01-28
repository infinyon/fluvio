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
use crate::common::output::Terminal;
use crate::common::OutputFormat;

/// List all existing SmartModules
#[derive(Debug, Parser)]
pub struct WatchSmartModuleOpt {
    #[clap(flatten)]
    output: OutputFormat,
}

#[async_trait]
impl ClientCmd for WatchSmartModuleOpt {
    async fn process_client<O: Terminal + Debug + Send + Sync>(
        self,
        _out: Arc<O>,
        fluvio: &Fluvio,
    ) -> Result<()> {
        let admin = fluvio.admin().await;

        let mut watch_stream = admin.watch::<SmartModuleSpec>().await?;
        loop {
            select! {
                next = watch_stream.next() => {
                    if let Some(Ok(event)) = next {
                        // low level printing, should be replaced
                        println!("SmartModule event: {event:?}");
                    } else {
                        println!("SmartModule event stream ended");
                        break;
                    }
                }
            }
        }
        Ok(())
    }
}
