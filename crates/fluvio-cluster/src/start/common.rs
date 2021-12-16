use std::{env, time::Duration};

use fluvio_controlplane_metadata::spu::SpuSpec;
use k8_client::{SharedK8Client, ClientError};
use once_cell::sync::Lazy;
use semver::Version;
use tracing::{debug, error, instrument};

use fluvio::{Fluvio, FluvioConfig};
use fluvio_future::timer::sleep;

/// maximum time for VERSION CHECK
static MAX_SC_LOOP: Lazy<u8> = Lazy::new(|| {
    let var_value = env::var("FLV_CLUSTER_MAX_SC_VERSION_LOOP").unwrap_or_default();
    var_value.parse().unwrap_or(60)
});

/// try connection to SC
#[instrument]
pub async fn try_connect_to_sc(
    config: &FluvioConfig,
    platform_version: &Version,
) -> Option<Fluvio> {

    enum ScConnectionState {
        Connected(Fluvio),
        NotConnected,
        VersionMismatch,
        TimeOut,
    }


    async fn try_connect_sc(
        fluvio_config: &FluvioConfig,
        expected_version: &Version,
    ) -> ScConnectionState {
        use tokio::select;

        select! {
            _ = &mut sleep(Duration::from_secs(10)) => {
                debug!("timer expired");
                ScConnectionState::TimeOut
            },

            connection = Fluvio::connect_with_config(fluvio_config) =>  {

                match connection {
                    Ok(fluvio) => {
                        let current_version = fluvio.platform_version();
                        if current_version == expected_version {
                            debug!("Got updated SC Version{}", &expected_version);
                            ScConnectionState::Connected(fluvio)
                        } else {
                            // This state will never change if we identify it, so exit early
                            error!("Current Version {} is not same as expected: {}",current_version,expected_version);
                            ScConnectionState::VersionMismatch
                        }
                    }
                    Err(err) => {
                        debug!("couldn't connect: {:#?}", err);
                        ScConnectionState::NotConnected
                    }
                }

            }
        }
    }

    for attempt in 0..*MAX_SC_LOOP {
        debug!(
            "Trying to connect to sc at: {}, attempt: {}",
            config.endpoint, attempt
        );

        match try_connect_sc(config, platform_version).await {
            ScConnectionState::Connected(fluvio) => {
                debug!("Connection to sc succeeded!");
                return Some(fluvio);
            },
            ScConnectionState::VersionMismatch => {
                return None;
            },
            _ => {
                // Do nothing, let the connection loop spin
            },
        };


        if attempt < *MAX_SC_LOOP - 1 {
            debug!("Connection failed. Sleeping 10 seconds");
            sleep(Duration::from_secs(10)).await;
        }
    }

    error!("fail to connect to sc at: {}", config.endpoint);
    None
}

// hack
pub async fn check_crd(client: SharedK8Client) -> Result<(), ClientError> {
    use k8_metadata_client::MetadataClient;

    for i in 0..100 {
        debug!("checking fluvio crd attempt: {}", i);
        // check if spu is installed
        if let Err(err) = client.retrieve_items::<SpuSpec, _>("default").await {
            debug!("problem retrieving fluvio crd {}", err);
            sleep(Duration::from_secs(10)).await;
        } else {
            debug!("fluvio crd installed");
            return Ok(());
        }
    }

    Err(ClientError::Other("Fluvio CRD not ready".to_string()))
}
