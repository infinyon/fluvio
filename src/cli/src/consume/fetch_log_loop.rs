//!
//! # SPU - Fetch Log
//!
//! Fetch logs from SPU
//!

use std::io::Error as IoError;
use std::io::ErrorKind;
use std::convert::TryFrom;

use tracing::debug;
use fluvio::{PartitionConsumer, Offset, ConsumerConfig};
use futures_lite::StreamExt;

use crate::error::CliError;
use crate::Terminal;

use crate::consume::ConsumeLogOpt;
use crate::consume::logs_output::process_fetch_topic_response;

// -----------------------------------
// SPU - Fetch Loop
// -----------------------------------

/// Fetch log continuously
#[allow(clippy::neg_multiply)]
pub async fn fetch_log_loop<O>(
    out: std::sync::Arc<O>,
    consumer: PartitionConsumer,
    opt: ConsumeLogOpt,
) -> Result<(), CliError>
where
    O: Terminal,
{
    debug!("starting fetch loop: {:#?}", opt);

    // attach sender to Ctrl-C event handler
    if let Err(err) = ctrlc::set_handler(move || {
        debug!("detected control c, setting end");
        std::process::exit(0);
    }) {
        return Err(IoError::new(
            ErrorKind::InvalidData,
            format!("CTRL-C handler can't be initialized {}", err),
        )
        .into());
    }

    // compute offset
    let maybe_initial_offset = if opt.from_beginning {
        let big_offset = opt.offset.unwrap_or(0);
        // Try to convert to u32
        u32::try_from(big_offset).ok().map(Offset::from_beginning)
    } else if let Some(big_offset) = opt.offset {
        // if it is negative, we start from end
        if big_offset < 0 {
            // Try to convert to u32
            u32::try_from(big_offset * -1).ok().map(Offset::from_end)
        } else {
            Offset::absolute(big_offset).ok()
        }
    } else {
        Some(Offset::end())
    };

    let initial_offset = match maybe_initial_offset {
        Some(offset) => offset,
        None => {
            return Err(CliError::InvalidArg("Illegal offset. Relative offsets must be u32 and absolute offsets must be positive".to_string()));
        }
    };

    let fetch_config = {
        let mut config = ConsumerConfig::default();
        if let Some(max_bytes) = opt.max_bytes {
            config = config.with_max_bytes(max_bytes);
        }
        config
    };

    if opt.disable_continuous {
        let response = consumer
            .fetch_with_config(initial_offset, fetch_config)
            .await?;

        debug!(
            "got a single response: LSO: {} batches: {}",
            response.log_start_offset,
            response.records.batches.len(),
        );

        process_fetch_topic_response(out.clone(), response, &opt).await?;
    } else {
        let mut log_stream = consumer
            ._stream_batches_with_config(initial_offset, fetch_config)
            .await?;

        while let Some(Ok(response)) = log_stream.next().await {
            let partition = response.partition;
            debug!(
                "got response: LSO: {} batchs: {}",
                partition.log_start_offset,
                partition.records.batches.len(),
            );

            process_fetch_topic_response(out.clone(), partition, &opt).await?;

            if opt.disable_continuous {
                debug!("finishing fetch loop");
                break;
            }
        }

        debug!("fetch loop exited");
    }

    Ok(())
}
