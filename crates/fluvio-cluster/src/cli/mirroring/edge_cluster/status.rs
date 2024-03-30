use std::sync::Arc;
use clap::Parser;
use fluvio_extension_common::{target::ClusterTarget, Terminal};
use anyhow::Result;

#[derive(Debug, Parser)]
pub struct StatusOpt {}

impl StatusOpt {
    pub async fn execute<T: Terminal>(
        self,
        _out: Arc<T>,
        _cluster_target: ClusterTarget,
    ) -> Result<()> {
        //list all upstreams
        //RemoteClusterStatus
        //
        todo!()
    }
}
