use std::{env, time::Duration};

use once_cell::sync::Lazy;
use tracing::{instrument};

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
        println!("trying to connec to sc: {}", fluvio_config.endpoint);
        match Fluvio::connect_with_config(fluvio_config).await {
            Ok(fluvio) => Some(fluvio),
            Err(err) => {
                println!("couldn't connect: {:#?}", err);
                return None;
            }
        }
    }

    println!("trying to connec to sc at {}", config.endpoint);

    for attempt in 0..*MAX_SC_LOOP {
        if let Some(fluvio) = try_connect_sc(config).await {
            return Some(fluvio);
        } else {
            if attempt < *MAX_SC_LOOP - 1 {
                println!("sleeping 10 seconds. attemp: {}", attempt);
                sleep(Duration::from_secs(10)).await;
            }
        }
    }

    println!("fail to connect to sc at: {}", config.endpoint);
    None
}
