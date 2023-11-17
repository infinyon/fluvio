use std::fmt::Debug;
use std::sync::Arc;

use clap::Parser;
use fluvio::Fluvio;
use fluvio_cli_common::smartmodule::{BaseTestCmd, WithChainBuilder};
use fluvio_extension_common::Terminal;

use crate::client::cmd::ClientCmd;

#[derive(Debug, Parser)]
#[command(arg_required_else_help = true)]
pub struct TestSmartModuleOpt {
    #[clap(flatten)]
    base: BaseTestCmd,
}

#[async_trait::async_trait]
impl ClientCmd for TestSmartModuleOpt {
    async fn process_client<O: Terminal + Debug + Send + Sync>(
        self,
        _out: Arc<O>,
        _fluvio: &Fluvio,
    ) -> anyhow::Result<()> {
        self.base
            .process::<fn(_, _) -> _>(WithChainBuilder::default())
            .await
    }
}
