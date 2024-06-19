use std::{
    env,
    time::{Duration, SystemTime},
};

use fluvio_controlplane_metadata::spu::SpuSpec;
use k8_client::SharedK8Client;
use once_cell::sync::Lazy;
use semver::Version;
use tracing::{debug, error, info, instrument, warn};

use fluvio::{Fluvio, FluvioConfig};
use fluvio_future::{
    retry::{retry, ExponentialBackoff, RetryExt},
    timer::sleep,
};

use crate::render::ProgressRenderer;

/// maximum time for VERSION CHECK
static MAX_SC_LOOP: Lazy<u8> = Lazy::new(|| {
    let var_value = env::var("FLV_CLUSTER_MAX_SC_VERSION_LOOP").unwrap_or_default();
    var_value.parse().unwrap_or(120)
});

#[derive(Debug)]
enum TryConnectError {
    Timeout,
    #[allow(dead_code)]
    UnexpectedVersion {
        expected: Version,
        current: Version,
    },
    #[allow(dead_code)]
    Unexpected(anyhow::Error),
}

/// try connection to SC
#[instrument]
pub async fn try_connect_to_sc(
    config: &FluvioConfig,
    platform_version: &Version,
    pb: &ProgressRenderer,
) -> Option<Fluvio> {
    async fn try_connect_sc(
        fluvio_config: &FluvioConfig,
        expected_version: &Version,
    ) -> Result<Fluvio, TryConnectError> {
        match Fluvio::connect_with_config(fluvio_config).await {
            Ok(fluvio) => {
                let current_version = fluvio.platform_version();
                if current_version == expected_version {
                    debug!(version = %current_version, "Got updated SC Version");
                    Ok(fluvio)
                } else {
                    warn!(
                        "Current Version {} is not same as expected: {}",
                        current_version, expected_version
                    );
                    Err(TryConnectError::UnexpectedVersion {
                        expected: expected_version.clone(),
                        current: current_version.clone(),
                    })
                }
            }
            Err(err) => {
                warn!("couldn't connect: {:#?}", err);
                Err(TryConnectError::Unexpected(err))
            }
        }
    }

    let mut attempt = 0u16;
    let time = SystemTime::now();
    let operation = || {
        attempt += 1;

        debug!(
            "Trying to connect to sc at: {}, attempt: {}",
            config.endpoint, attempt
        );
        let elapsed = time.elapsed().unwrap();
        pb.set_message(format!(
            "🖥️  Trying to connect to SC: {} {} seconds elapsed",
            config.endpoint,
            elapsed.as_secs()
        ));
        async move {
            let retry_timeout = 10;
            match try_connect_sc(config, platform_version)
                .timeout(Duration::from_secs(retry_timeout))
                .await
                .unwrap_or(Err(TryConnectError::Timeout))
            {
                Ok(fluvio) => {
                    info!("Connection to sc succeed!");
                    Ok(fluvio)
                }
                Err(err) => {
                    warn!("Connection failed with {:?}", err);
                    Err(err)
                }
            }
        }
    };

    retry(
        ExponentialBackoff::from_millis(2)
            .max_delay(Duration::from_secs(10))
            .take(*MAX_SC_LOOP as usize),
        operation,
    )
    .await
    .map_err(|_| error!("fail to connect to sc at: {}", config.endpoint))
    .ok()
}

// hack
pub async fn check_crd(client: SharedK8Client) -> anyhow::Result<()> {
    use k8_client::meta_client::MetadataClient;

    for i in 0..100 {
        debug!("checking fluvio crd attempt: {}", i);
        // check if spu is installed
        if let Err(err) = client.retrieve_items::<SpuSpec, _>("default").await {
            debug!("problem retrieving fluvio crd {}", err);
            sleep(Duration::from_secs(1)).await;
        } else {
            debug!("fluvio crd installed");
            return Ok(());
        }
    }

    Err(anyhow::anyhow!("Fluvio CRD not ready"))
}
