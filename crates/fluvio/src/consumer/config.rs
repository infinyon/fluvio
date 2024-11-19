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

#[derive(Debug, Default, Copy, Clone, PartialEq)]
pub enum OffsetManagementStrategy {
    /// Offsets are not saved
    #[default]
    None,
    /// All operations must be invoked explicitly.
    Manual,
    /// Before yielding a new record to the caller, the previous record is committed and flushed if the configured interval is passed.
    /// Additionally, the commit and the flush are triggered when the stream object gets dropped.
    Auto,
}

#[derive(Debug, Builder, Clone)]
#[builder(build_fn(private, name = "build_impl"))]
pub struct ConsumerConfigExt {
    #[builder(setter(into))]
    pub topic: String,
    #[builder(default, setter(custom))]
    pub partition: Vec<PartitionId>,
    #[builder(default, setter(strip_option, into))]
    pub mirror: Option<String>,
    #[builder(default, setter(strip_option, into))]
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
            mirror: _,
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

impl ConsumerConfigExtBuilder {
    pub fn build(&self) -> Result<ConsumerConfigExt> {
        let config = self.build_impl().map_err(|e| {
            FluvioError::ConsumerConfig(format!("Missing required config option: {e}"))
        })?;

        if config.offset_strategy != OffsetManagementStrategy::None
            && config.offset_consumer.is_none()
        {
            return Err((FluvioError::ConsumerConfig(
                "Consumer id is required when using offset strategy".to_owned(),
            ))
            .into());
        }

        Ok(config)
    }

    pub fn partition(&mut self, value: PartitionId) -> &mut Self {
        self.partition.get_or_insert(Vec::new()).push(value);
        self
    }
}

impl From<ConsumerConfigExt> for ConsumerConfig {
    fn from(value: ConsumerConfigExt) -> Self {
        let ConsumerConfigExt {
            topic: _,
            partition: _,
            mirror: _,
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
