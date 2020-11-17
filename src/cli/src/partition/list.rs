//!
//! # List All Partition
//!
//! CLI tree and processing to list SPUs
//!

use structopt::StructOpt;

use fluvio::Fluvio;
use fluvio_controlplane_metadata::partition::*;

use crate::{Result, Terminal};
use crate::common::OutputFormat;

/// Option for Listing Partition
#[derive(Debug, StructOpt)]
pub struct ListPartitionOpt {
    #[structopt(flatten)]
    output: OutputFormat,
}

impl ListPartitionOpt {
    /// perform actions
    pub async fn process<O>(self, out: std::sync::Arc<O>, fluvio: &Fluvio) -> Result<()>
    where
        O: Terminal,
    {
        let output = self.output.format;
        let mut admin = fluvio.admin().await;

        let partitions = admin.list::<PartitionSpec, _>(vec![]).await?;

        // format and dump to screen
        display::format_partition_response_output(out, partitions, output)?;
        Ok(())
    }
}

mod display {

    use std::convert::TryInto;

    use prettytable::Row;
    use prettytable::row;
    use prettytable::cell;

    use fluvio::metadata::objects::Metadata;
    use fluvio::metadata::partition::*;
    use fluvio::dataplane::PartitionError;

    use crate::error::CliError;
    use crate::OutputType;
    use crate::Terminal;
    use crate::TableOutputHandler;
    use crate::t_println;

    type ListSpus = Vec<Metadata<PartitionSpec>>;

    /// Process server based on output type
    pub fn format_partition_response_output<O>(
        out: std::sync::Arc<O>,
        spus: ListSpus,
        output_type: OutputType,
    ) -> Result<(), CliError>
    where
        O: Terminal,
    {
        if !spus.is_empty() {
            out.render_list(&spus, output_type)?;
        } else {
            t_println!(out, "No partitions found");
        }

        Ok(())
    }

    impl TableOutputHandler for ListSpus {
        /// table header implementation
        fn header(&self) -> Row {
            row![
                "TOPIC",
                "PARTITION",
                "LEADER",
                "REPLICAS",
                "RESOLUTION",
                "HW",
                "LEO",
                "LSR",
                "FOLLOWER OFFSETS"
            ]
        }

        /// return errors in string format
        fn errors(&self) -> Vec<String> {
            vec![]
        }

        fn content(&self) -> Vec<Row> {
            self.iter()
                .map(|metadata| {
                    let spec = &metadata.spec;
                    let status = &metadata.status;
                    let (topic, partition) = {
                        let parse_key: Result<ReplicaKey, PartitionError> =
                            metadata.name.clone().try_into();
                        match parse_key {
                            Ok(key) => key.split(),
                            Err(err) => (err.to_string(), -1),
                        }
                    };

                    row![
                        l -> topic,
                        l -> partition.to_string(),
                        l -> spec.leader.to_string(),
                        l -> format!("{:?}",spec.followers()),
                        l -> format!("{:?}",status.resolution),
                        l -> status.leader.hw.to_string(),
                        l -> status.leader.leo.to_string(),
                        l -> status.lsr.to_string(),
                        l -> format!("{:?}",status.replicas)
                    ]
                })
                .collect()
        }
    }
}
