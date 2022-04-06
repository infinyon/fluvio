//!
//! # List All Partition
//!
//! CLI tree and processing to list SPUs
//!

use clap::Parser;

use fluvio::Fluvio;
use fluvio::metadata::partition::*;

use crate::Result;
use crate::common::output::Terminal;
use crate::common::OutputFormat;

/// Option for Listing Partition
#[derive(Debug, Parser)]
pub struct ListPartitionOpt {
    #[clap(flatten)]
    output: OutputFormat,
}

impl ListPartitionOpt {
    /// perform actions
    pub async fn process<O>(self, out: std::sync::Arc<O>, fluvio: &Fluvio) -> Result<()>
    where
        O: Terminal,
    {
        let output = self.output.format;
        let admin = fluvio.admin().await;

        let partitions = admin.list::<PartitionSpec, _>(vec![]).await?;

        // format and dump to screen
        display::format_partition_response_output(out, partitions, output)?;
        Ok(())
    }
}

mod display {

    use std::convert::TryInto;

    use comfy_table::Row;

    use serde::Serialize;

    use fluvio::metadata::objects::Metadata;
    use fluvio::metadata::partition::*;
    use fluvio::dataplane::PartitionError;

    //use crate::error::CliError;
    use crate::common::t_println;
    use crate::common::output::{OutputType, OutputError, Terminal, TableOutputHandler};

    #[derive(Serialize)]
    struct ListSpus(Vec<Metadata<PartitionSpec>>);

    impl IntoIterator for ListSpus {
        type Item = Metadata<PartitionSpec>;
        type IntoIter = std::vec::IntoIter<Self::Item>;

        fn into_iter(self) -> Self::IntoIter {
            self.0.into_iter()
        }
    }

    /// Process server based on output type
    pub fn format_partition_response_output<O>(
        out: std::sync::Arc<O>,
        spus: Vec<Metadata<PartitionSpec>>,
        output_type: OutputType,
    ) -> Result<(), OutputError>
    where
        O: Terminal,
    {
        if !spus.is_empty() {
            let meta_spus = ListSpus(spus);
            out.render_list(&meta_spus, output_type)?;
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
                "SIZE",
                "HW",
                "LEO",
                "LRS",
                "FOLLOWER OFFSETS"
            ]
        }

        /// return errors in string format
        fn errors(&self) -> Vec<String> {
            vec![]
        }

        fn content(&self) -> Vec<Row> {
            let mut metadata = self.0.clone();
            metadata.sort_by(|a, b| a.name.cmp(&b.name));
            metadata
                .iter()
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

                    let printable_size = match status.size {
                        PartitionStatus::SIZE_NOT_SUPPORTED => "NA".to_string(),
                        PartitionStatus::SIZE_ERROR => "ERROR".to_string(),
                        _ => human_bytes::human_bytes(status.size as f64),
                    };

                    let mut row = Row::new();

                    row.add_cell(topic);
                    row.add_cell(partition.to_string());
                    row.add_cell(spec.leader.to_string());
                    row.add_cell(format!("{:?}",spec.followers()));
                    row.add_cell(format!("{:?}",status.resolution));
                    row.add_cell(printable_size);
                    row.add_cell(status.leader.hw.to_string());
                    row.add_cell(status.leader.leo.to_string());
                    row.add_cell(status.leader.leo.to_string());
                    row.add_cell(status.lsr.to_string());
                    row.add_cell(format!("{:?}",status.replicas));
                })
                .collect()
        }
    }
}
