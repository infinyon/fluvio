//!
//! # SPU - Fetch Log
//!
//! Fetch logs from SPU
//!

use std::io::Error as IoError;
use std::io::ErrorKind;

use log::debug;

use flv_client::params::*;
use flv_client::Consumer;

use crate::error::CliError;
use crate::Terminal;

use super::ConsumeLogConfig;
use super::process_fetch_topic_response;

// -----------------------------------
// SPU - Fetch Loop
// -----------------------------------

/// Fetch log continuously
pub async fn fetch_log_loop<O>(
    out: std::sync::Arc<O>,
    mut consumer: Consumer,
    mut opt: ConsumeLogConfig,
) -> Result<(), CliError>
where
    O: Terminal,
{
    
    // force to be non continous
    opt.disable_continuous = true;

    debug!("starting fetch loop: {:#?}", opt);

    // attach sender to Ctrl-C event handler
    if let Err(err) = ctrlc::set_handler(move || {
        debug!("detected control c, setting end");
        std::process::exit(0);
    }) {
        return Err(CliError::IoError(IoError::new(
            ErrorKind::InvalidData,
            format!("CTRL-C handler can't be initialized {}", err),
        )));
    }

    // compute offset
    let initial_offset = if opt.from_beginning {
        FetchOffset::Earliest(opt.offset)
    } else {
        if let Some(offset) = opt.offset {
            // if it is negative, we start from end
            if offset < 0 {
                FetchOffset::Latest(Some(offset * -1))
            } else {
                FetchOffset::Offset(offset)
            }
        } else {
            FetchOffset::Latest(None)
        }
    };

    let fetch_option = FetchLogOption {
        max_bytes: opt.max_bytes,
        ..Default::default()
    };

    if opt.disable_continuous {
        let response = consumer.fetch_logs_once(initial_offset, fetch_option).await?;

        debug!(
            "got a single response: LSO: {} batches: {}",
            response.log_start_offset,
            response.records.batches.len(),
        );

        process_fetch_topic_response(out.clone(),consumer.topic(),response, &opt).await?;
    } else {
        /*
        let mut log_stream = leader.fetch_logs(initial_offset, fetch_option);

        while let Some(response) = log_stream.next().await {
            debug!(
                "got response: LSO: {} batchs: {}",
                response.log_start_offset,
                response.records.batches.len(),
            );

            process_fetch_topic_response(out.clone(), &topic, response, &opt).await?;

            if opt.disable_continuous {
                debug!("finishing fetch loop");
                break;
            }
        }
        */

        debug!("fetch loop exited");
    }

    Ok(())
}
