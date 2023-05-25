// schema/apply.rs

use std::sync::Arc;
use std::fmt::Debug;

use clap::Parser;
use anyhow::Result;

use fluvio_extension_common::Terminal;

/// Apply schema to topic
#[derive(Debug, Parser)]
pub struct ApplySchemaOpt {
    #[arg(value_name = "topic", required = true)]
    topic: String,
}

impl ApplySchemaOpt {
    pub async fn process<O: Terminal + Debug + Send + Sync>(self, _out: Arc<O>) -> Result<()> {
        Ok(())
    }
}
