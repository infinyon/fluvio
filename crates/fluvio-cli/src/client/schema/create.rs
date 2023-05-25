// schema/create.rs

use std::sync::Arc;
use std::fmt::Debug;

use clap::Parser;
use anyhow::Result;

use fluvio_extension_common::Terminal;

/// Create schema given a schema config
#[derive(Debug, Parser)]
pub struct CreateSchemaOpt {
    #[arg(long, required = true)]
    config: String,
}

impl CreateSchemaOpt {
    pub async fn process<O: Terminal + Debug + Send + Sync>(self, _out: Arc<O>) -> Result<()> {
        Ok(())
    }
}
