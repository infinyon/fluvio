use std::time::Duration;

use anyhow::Result;
use derive_builder::Builder;

use fluvio_spu_schema::{server::smartmodule::SmartModuleInvocation, Isolation};
use fluvio_types::PartitionId;

use crate::{FluvioError, Offset};

use super::MAX_FETCH_BYTES;

const DEFAULT_OFFSET_FLUSH_PERIOD: Duration = Duration::from_secs(10);

/// Configures the behavior of consumer fetching and streaming
#[derive(Debug, Builder, Clone)]
#[builder(build_fn(private, name = "build_impl"))]
pub struct ConsumerConfig {
    #[builder(default)]
    pub disable_continuous: bool,
    #[builder(default = "*MAX_FETCH_BYTES")]
    pub max_bytes: i32,
    #[builder(default)]
    pub isolation: Isolation,
    #[builder(default)]
    pub smartmodule: Vec<SmartModuleInvocation>,
}

impl ConsumerConfig {
    pub fn builder() -> ConsumerConfigBuilder {
        ConsumerConfigBuilder::default()
    }
}

impl ConsumerConfigBuilder {
    pub fn build(&self) -> Result<ConsumerConfig> {
        let config = self.build_impl().map_err(|e| {
            FluvioError::ConsumerConfig(format!("Missing required config option: {e}"))
        })?;
        Ok(config)
    }
}

#[derive(Debug, Default, Copy, Clone)]
pub enum OffsetManagementStrategy {
    #[default]
    None,
    Manual,
    Auto,
}

#[derive(Debug, Builder, Clone)]
pub struct ConsumerConfigExt {
    pub topic: String,
    #[builder(default, setter(strip_option))]
    pub partition: Option<PartitionId>,
    #[builder(default, setter(strip_option))]
    pub offset_consumer: Option<String>,
    pub offset_start: Offset,
    #[builder(default)]
    pub offset_strategy: OffsetManagementStrategy,
    #[builder(default = "DEFAULT_OFFSET_FLUSH_PERIOD")]
    pub offset_flush: Duration,
    #[builder(default)]
    disable_continuous: bool,
    #[builder(default = "*MAX_FETCH_BYTES")]
    pub max_bytes: i32,
    #[builder(default)]
    pub isolation: Isolation,
    #[builder(default)]
    pub smartmodule: Vec<SmartModuleInvocation>,
}

impl ConsumerConfigExt {
    pub fn builder() -> ConsumerConfigExtBuilder {
        ConsumerConfigExtBuilder::default()
    }

    pub fn into_parts(
        self,
    ) -> (
        Offset,
        ConsumerConfig,
        Option<String>,
        OffsetManagementStrategy,
        Duration,
    ) {
        let Self {
            topic: _,
            partition: _,
            offset_consumer,
            offset_start,
            disable_continuous,
            max_bytes,
            isolation,
            smartmodule,
            offset_strategy,
            offset_flush,
        } = self;

        let config = ConsumerConfig {
            disable_continuous,
            max_bytes,
            isolation,
            smartmodule,
        };

        (
            offset_start,
            config,
            offset_consumer,
            offset_strategy,
            offset_flush,
        )
    }
}

impl From<ConsumerConfigExt> for ConsumerConfig {
    fn from(value: ConsumerConfigExt) -> Self {
        let ConsumerConfigExt {
            topic: _,
            partition: _,
            offset_consumer: _,
            offset_start: _,
            offset_strategy: _,
            offset_flush: _,
            disable_continuous,
            max_bytes,
            isolation,
            smartmodule,
        } = value;

        Self {
            disable_continuous,
            max_bytes,
            isolation,
            smartmodule,
        }
    }
}
