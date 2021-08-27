use std::{env, time::Duration};

use fluvio_controlplane_metadata::spu::SpuSpec;
use k8_client::{SharedK8Client, ClientError};
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
        use tokio::select;

        select! {
            _ = &mut sleep(Duration::from_secs(10)) => {
                debug!("timer expired");
                None
            },

            connection = Fluvio::connect_with_config(fluvio_config) =>  {

                match connection {
                    Ok(fluvio) => Some(fluvio),
                    Err(err) => {
                        debug!("couldn't connect: {:#?}", err);
                        None
                    }
                }

            }
        }
    }

    for attempt in 0..*MAX_SC_LOOP {
        println!(
            "Trying to connect to sc at: {}, attempt: {}",
            config.endpoint, attempt
        );
        if let Some(fluvio) = try_connect_sc(config).await {
            println!("Connection to sc suceed!");
            return Some(fluvio);
        } else if attempt < *MAX_SC_LOOP - 1 {
            println!("Connection failed.  sleeping 10 seconds");
            sleep(Duration::from_secs(10)).await;
        }
    }

    println!("fail to connect to sc at: {}", config.endpoint);
    None
}

// hack
pub async fn check_crd(client: SharedK8Client) -> Result<(), ClientError> {
    use k8_metadata_client::MetadataClient;

    for i in 0..100 {
        println!("checking fluvio crd attempt: {}", i);
        // check if spu is installed
        if let Err(err) = client.retrieve_items::<SpuSpec, _>("default").await {
            println!("problem retrieving fluvio crd {}", err);
            println!("sleeping 1 seconds");
            sleep(Duration::from_secs(10)).await;
        } else {
            println!("fluvio crd installed");
            return Ok(());
        }
    }

    Err(ClientError::Other("Fluvio CRD not ready".to_string()))
}
