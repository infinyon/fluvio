use std::{env, time::Duration};

use once_cell::sync::Lazy;
use tracing::{debug, instrument};

use fluvio::{Fluvio, FluvioConfig};
use fluvio_future::timer::sleep;

/// maximum tiime for VERSION CHECK
static MAX_SC_LOOP: Lazy<u8> = Lazy::new(|| {
    let var_value = env::var("FLV_CLUSTER_MAX_SC_VERSION_LOOP").unwrap_or_default();
    var_value.parse().unwrap_or(60)
});

/// try connection to SC
#[instrument]
pub async fn try_connect_to_sc(config: &FluvioConfig) -> Option<Fluvio> {
    async fn try_connect_sc(fluvio_config: &FluvioConfig) -> Option<Fluvio> {
        match Fluvio::connect_with_config(fluvio_config).await {
            Ok(fluvio) => Some(fluvio),
            Err(err) => {
                debug!("couldn't connect: {:#?}", err);
                return None;
            }
        }
    }

    for attempt in 0..*MAX_SC_LOOP {
        println!(
            "trying to connect to sc at: {}, attempt: {}",
            config.endpoint, attempt
        );
        if let Some(fluvio) = try_connect_sc(config).await {
            println!("connection to sc suceed!");
            return Some(fluvio);
        } else {
            if attempt < *MAX_SC_LOOP - 1 {
                println!("connection failed.  sleeping 10 seconds");
                sleep(Duration::from_secs(10)).await;
            }
        }
    }

    println!("fail to connect to sc at: {}", config.endpoint);
    None
}
