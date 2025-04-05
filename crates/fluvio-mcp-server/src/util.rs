use anyhow::{Context, Result};
use bon::Builder;
use fluvio::{Fluvio, FluvioConfig};

/*
pub(crate) enum Transport {
    Console,
    Sse,
}
*/

#[derive(Builder)]
pub(crate) struct FluvioConnect {
    #[builder(into)]
    profile: Option<String>,
}

impl FluvioConnect {
    pub(crate) async fn connect(&self) -> Result<Fluvio> {
        let config = match &self.profile {
            Some(profile) => {
                FluvioConfig::load_with_profile(profile)?.context("no fluvio profile")?
            }
            None => FluvioConfig::load()?,
        };

        Ok(Fluvio::connect_with_config(&config).await?)
    }
}
