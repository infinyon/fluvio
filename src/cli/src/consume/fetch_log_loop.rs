//!
//! # SPU - Fetch Log
//!
//! Fetch logs from SPU
//!

use std::io::Error as IoError;
use std::io::ErrorKind;
use std::time::Duration;
use std::sync::RwLock;
use std::sync::Arc;

use log::debug;

use flv_future_core::sleep;
use kf_protocol::api::PartitionOffset;
use fluvio_client::ReplicaLeader;

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

    // let (sender, mut receiver) = mpsc::channel::<bool>(5);
    let end = Arc::new(RwLock::new(false));

    let _end2 = end.clone();
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
    let offsets = leader.fetch_offsets().await?;
    let last_offset = offsets.last_stable_offset();

    let mut current_offset = if opt.from_beginning {
        offsets.start_offset()
    } else {
        offsets.last_stable_offset()
    };

    debug!("entering loop");

    loop {
        debug!("fetching with offset: {}", current_offset);

        let fetch_logs_res = leader.fetch_logs(current_offset, opt.max_bytes).await?;

        current_offset = fetch_logs_res.last_stable_offset;

        debug!("fetching last offset: {} and response", last_offset);

        // process logs response

        process_fetch_topic_response(out.clone(), &topic, fetch_logs_res, &opt).await?;
        let read_lock = end.read().unwrap();
        if *read_lock {
            debug!("detected end by ctrl-c, exiting loop");
            break;
        }

        /*
        if !opt.continuous {
            if last_offset > last_offset {
                debug!("finishing fetch loop");
                break;
            }
        }
        */

        
        if !opt.continuous {
            debug!("finishing fetch loop");
            break;
        }
        

        debug!("sleeping 200 ms between next fetch");
        sleep(Duration::from_millis(200)).await;
    }

    debug!("fetch loop exited");
    Ok(())
}
