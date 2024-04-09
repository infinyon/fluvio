use clap::Parser;
use anyhow::Result;

use fluvio::Fluvio;

use crate::common::output::Terminal;
use crate::common::OutputFormat;

/// Option for Listing Consumers
#[derive(Debug, Parser)]
pub struct ListConsumerOpt {
    #[clap(flatten)]
    output: OutputFormat,
}

impl ListConsumerOpt {
    pub async fn process<O>(self, out: std::sync::Arc<O>, fluvio: &Fluvio) -> Result<()>
    where
        O: Terminal,
    {
        let consumers = fluvio.consumer_offsets().await?;

        display::format_response_output(out, consumers, self.output.format)?;
        Ok(())
    }
}

mod display {

    use std::time::{Duration, SystemTime};

    use comfy_table::{Row, Cell};

    use fluvio::consumer::ConsumerOffset;
    use serde::Serialize;

    use crate::common::t_println;
    use crate::common::output::{OutputType, OutputError, Terminal, TableOutputHandler};

    #[derive(Serialize)]
    struct ListConsumers(Vec<ConsumerOffset>);

    impl IntoIterator for ListConsumers {
        type Item = ConsumerOffset;
        type IntoIter = std::vec::IntoIter<Self::Item>;

        fn into_iter(self) -> Self::IntoIter {
            self.0.into_iter()
        }
    }

    pub fn format_response_output<O>(
        out: std::sync::Arc<O>,
        consumers: Vec<ConsumerOffset>,
        output_type: OutputType,
    ) -> Result<(), OutputError>
    where
        O: Terminal,
    {
        if !consumers.is_empty() {
            out.render_list(&ListConsumers(consumers), output_type)?;
        } else {
            t_println!(out, "No consumers found");
        }

        Ok(())
    }

    impl TableOutputHandler for ListConsumers {
        fn header(&self) -> Row {
            Row::from(["CONSUMER", "TOPIC", "PARTITION", "OFFSET", "LAST SEEN"])
        }

        fn errors(&self) -> Vec<String> {
            vec![]
        }

        fn content(&self) -> Vec<Row> {
            let now = SystemTime::now()
                .duration_since(SystemTime::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs();
            let mut list = self.0.clone();
            list.sort();
            list.into_iter()
                .map(|consumer| {
                    let ConsumerOffset {
                        consumer_id,
                        offset,
                        modified_time,
                        topic,
                        partition,
                    } = consumer;
                    let last_seen =
                        humantime::Duration::from(Duration::from_secs(now - modified_time));
                    Row::from([
                        Cell::new(consumer_id),
                        Cell::new(topic),
                        Cell::new(partition),
                        Cell::new(offset),
                        Cell::new(last_seen),
                    ])
                })
                .collect()
        }
    }
}
