//!
//! # SPU - Fetch Log
//!
//! Fetch logs from SPU
//!

use std::io::Error as IoError;
use std::io::ErrorKind;

use log::debug;
use futures::stream::StreamExt;

use flv_client::ReplicaLeader;
use flv_client::FetchLogOption;
use flv_client::FetchOffset;

use crate::error::CliError;
use crate::Terminal;

use super::ConsumeLogConfig;
use super::process_fetch_topic_response;

// -----------------------------------
// SPU - Fetch Loop
// -----------------------------------

/// Fetch log continuously
pub async fn fetch_log_loop<O, L>(
    out: std::sync::Arc<O>,
    mut leader: L,
    opt: ConsumeLogConfig,
) -> Result<(), CliError>
where
    L: ReplicaLeader,
    O: Terminal,
{
    let topic = leader.topic().to_owned();
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

    // list offsets
    let initial_offset = if opt.from_beginning {
        FetchOffset::Earliest
    } else {
       FetchOffset::Latest
    };


    let fetch_option = FetchLogOption {
        max_bytes: opt.max_bytes,
        ..Default::default()
    };

    let mut log_stream = leader.fetch_logs(initial_offset, fetch_option);

    while let Some(record) = log_stream.next().await {
        process_fetch_topic_response(out.clone(), &topic, record, &opt).await?;

        if !opt.continuous {
            debug!("finishing fetch loop");
            break;
        }
    }

    debug!("fetch loop exited");
    Ok(())
}
