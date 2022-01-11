use std::{
    env,
    time::{Duration, SystemTime},
};

use fluvio_controlplane_metadata::spu::SpuSpec;
use k8_client::{SharedK8Client, ClientError};
use once_cell::sync::Lazy;
use semver::Version;
use tracing::{debug, error, instrument, warn};

use fluvio::{Fluvio, FluvioConfig};
use fluvio_future::timer::sleep;

use crate::render::ProgressRenderer;

/// maximum time for VERSION CHECK
static MAX_SC_LOOP: Lazy<u8> = Lazy::new(|| {
    let var_value = env::var("FLV_CLUSTER_MAX_SC_VERSION_LOOP").unwrap_or_default();
    var_value.parse().unwrap_or(120)
});

/// try connection to SC
#[instrument(skip(config, pb))]
pub async fn try_connect_to_sc(
    config: &FluvioConfig,
    platform_version: &Version,
    pb: &ProgressRenderer,
) -> Option<Fluvio> {
    async fn try_connect_sc(
        fluvio_config: &FluvioConfig,
        expected_version: &Version,
    ) -> Option<Fluvio> {
        use tokio::select;

        select! {
            _ = &mut sleep(Duration::from_secs(10)) => {
                debug!("timer expired");
                None
            },

            connection = Fluvio::connect_with_config(fluvio_config) =>  {

                match connection {
                    Ok(fluvio) => {
                        let current_version = fluvio.platform_version();
                        if current_version == expected_version {
                            debug!(%expected_version, "Got updated SC Version");
                            Some(fluvio)
                        } else {
                            warn!(%current_version, %expected_version, "Current Version is not same as expected");
                            None
                        }
                    }
                    Err(err) => {
                        debug!(?err, "couldn't connect");
                        None
                    }
                }

            }
        }
    }

    let time = SystemTime::now();
    for attempt in 0..*MAX_SC_LOOP {
        debug!(
            endpoint=%config.endpoint,
            attempt,
            "Trying to connect to sc",
        );
        let elapsed = time.elapsed().unwrap();
        pb.set_message(format!(
            "🖥️  Trying to connect to SC: {} {} seconds elapsed",
            config.endpoint,
            elapsed.as_secs()
        ));
        if let Some(fluvio) = try_connect_sc(config, platform_version).await {
            debug!("Connection to sc suceeded!");
            return Some(fluvio);
        } else if attempt < *MAX_SC_LOOP - 1 {
            debug!("Connection failed.  sleeping 10 seconds");
            sleep(Duration::from_secs(1)).await;
        }
    }

    error!(endpoint = %config.endpoint, "fail to connect to sc", );
    None
}

// hack
pub async fn check_crd(client: SharedK8Client) -> Result<(), ClientError> {
    use k8_metadata_client::MetadataClient;

    for i in 0..100 {
        debug!(attempt = i, "checking fluvio crd attempt",);
        // check if spu is installed
        if let Err(err) = client.retrieve_items::<SpuSpec, _>("default").await {
            debug!(%err, "problem retrieving fluvio crd");
            sleep(Duration::from_secs(1)).await;
        } else {
            debug!("fluvio crd installed");
            return Ok(());
        }
    }

    Err(ClientError::Other("Fluvio CRD not ready".to_string()))
}
