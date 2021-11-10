//!
//! # SmartStream Spec
//!
//!

use std::fmt::Display;
use std::marker::PhantomData;

use dataplane::core::{Encoder, Decoder};
use fluvio_stream_model::core::Spec;
use fluvio_stream_model::{core::MetadataItem, store::LocalStore};
use tracing::trace;

use crate::smartmodule::{SmartModuleSpec};
use crate::topic::TopicSpec;

use super::metadata::SmartStreamValidationError;

/// SmartStream is unstable feature
#[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartStreamSpec {
    pub input: SmartStreamInputRef,
    #[cfg_attr(feature = "use_serde", serde(flatten))]
    pub steps: SmartStreamSteps,
}

impl SmartStreamSpec {
    // validat configuration
    pub async fn validate<'a, C>(
        &'a self,
        objects: &SmartStreamValidationInput<'a, C>,
    ) -> Result<(), SmartStreamValidationError>
    where
        C: MetadataItem,
    {
        trace!("validating inputs");
        self.input.validate(objects).await?;
        trace!("validating output");
        self.steps.validate(objects.modules).await?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SmartStreamInputRef {
    #[cfg_attr(feature = "use_serde", serde(rename = "topic"))]
    Topic(SmartStreamRef<TopicSpec>),
    #[cfg_attr(feature = "use_serde", serde(rename = "smartstream"))]
    SmartStream(SmartStreamRef<SmartStreamSpec>),
}

impl Default for SmartStreamInputRef {
    fn default() -> Self {
        SmartStreamInputRef::Topic(SmartStreamRef::default())
    }
}

impl Display for SmartStreamInputRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SmartStreamInputRef::Topic(ref topic) => write!(f, "Topic({})", topic),
            SmartStreamInputRef::SmartStream(ref stream) => write!(f, "SmartStream({})", stream),
        }
    }
}

impl SmartStreamInputRef {
    pub fn object_id(&self) -> String {
        match self {
            SmartStreamInputRef::Topic(ref topic) => format!("{}", topic.name),
            SmartStreamInputRef::SmartStream(ref stream) => format!("SmartStream::{}", stream.name),
        }
    }

    // validat configuration
    pub async fn validate<'a, C>(
        &'a self,
        objects: &SmartStreamValidationInput<'a, C>,
    ) -> Result<(), SmartStreamValidationError>
    where
        C: MetadataItem,
    {
        match self {
            SmartStreamInputRef::Topic(ref topic_ref) => {
                if !topic_ref.validate(objects.topics).await {
                    trace!(topic = %topic_ref.name,"topic not found");
                    return Err(SmartStreamValidationError::TopicNotFound(
                        topic_ref.name.clone(),
                    ));
                }
            }
            SmartStreamInputRef::SmartStream(ref smart_stream_ref) => {
                if !smart_stream_ref.validate(objects.smartstreams).await {
                    return Err(SmartStreamValidationError::SmartStreamNotFound(
                        smart_stream_ref.name.clone(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct SmartStreamRef<S>
where
    S: Spec + Default + Encoder + Decoder,
    S::IndexKey: Default + Encoder + Decoder,
{
    pub name: S::IndexKey,
    #[cfg_attr(feature = "use_serde", serde(skip))]
    data: PhantomData<S>,
}

impl<S> Display for SmartStreamRef<S>
where
    S: Spec + Default + Encoder + Decoder,
    S::IndexKey: Default + Encoder + Decoder + Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl<S> SmartStreamRef<S>
where
    S: Spec + Default + Encoder + Decoder,
    S::IndexKey: Default + Encoder + Decoder,
{
    pub fn new(name: S::IndexKey) -> Self {
        SmartStreamRef {
            name,
            data: PhantomData,
        }
    }
}

impl<S> SmartStreamRef<S>
where
    S: Spec + Default + Encoder + Decoder,
    S::IndexKey: Default + Encoder + Decoder,
{
    // validate reference by checking key
    pub async fn validate<C>(&self, store: &LocalStore<S, C>) -> bool
    where
        C: MetadataItem,
    {
        store.contains_key(&self.name).await
    }
}

pub struct SmartStreamValidationInput<'a, C>
where
    C: MetadataItem,
{
    pub topics: &'a LocalStore<TopicSpec, C>,
    pub smartstreams: &'a LocalStore<SmartStreamSpec, C>,
    pub modules: &'a LocalStore<SmartModuleSpec, C>,
}

#[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct SmartStreamSteps {
    pub steps: Vec<SmartStreamStep>,
}

impl Display for SmartStreamSteps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let modules: String = self.steps.iter().map(|t| t.to_string()).collect();
        write!(f, "{}", modules)
    }
}

impl SmartStreamSteps {
    async fn validate<'a, C>(
        &'a self,
        modules: &'a LocalStore<SmartModuleSpec, C>,
    ) -> Result<(), SmartStreamValidationError>
    where
        C: MetadataItem,
    {
        for step in &self.steps {
            let module = step.module();
            if !modules.contains_key(module).await {
                return Err(SmartStreamValidationError::SmartModuleNotFound(
                    module.to_string(),
                ));
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SmartStreamStep {
    #[cfg_attr(feature = "use_serde", serde(rename = "filter"))]
    Filter(SmartStreamModule),
    #[cfg_attr(feature = "use_serde", serde(rename = "map"))]
    Map(SmartStreamModule),
    #[cfg_attr(feature = "use_serde", serde(rename = "filterMap"))]
    FilterMap(SmartStreamModule),
    #[cfg_attr(feature = "use_serde", serde(rename = "aggregate"))]
    Aggregate(SmartStreamModule),
    #[cfg_attr(feature = "use_serde", serde(rename = "join"))]
    Join(SmartStreamJoinModule),
}

impl Default for SmartStreamStep {
    fn default() -> Self {
        SmartStreamStep::Filter(SmartStreamModule::default())
    }
}

impl Display for SmartStreamStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SmartStreamStep::Filter(module) => write!(f, "Filter({})", module),
            SmartStreamStep::Map(module) => write!(f, "Map({})", module),
            SmartStreamStep::FilterMap(module) => write!(f, "FilterMap({})", module),
            SmartStreamStep::Aggregate(module) => write!(f, "Aggregate({})", module),
            SmartStreamStep::Join(module) => write!(f, "Join({})", module),
        }
    }
}

impl SmartStreamStep {
    pub fn module(&self) -> &str {
        match self {
            SmartStreamStep::Filter(ref module) => &module.module,
            SmartStreamStep::Map(ref module) => &module.module,
            SmartStreamStep::FilterMap(ref module) => &module.module,
            SmartStreamStep::Aggregate(ref module) => &module.module,
            SmartStreamStep::Join(ref module) => &module.module,
        }
    }
}

/// Generic SmartStream Module
#[derive(Debug, Clone, Default, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "use_serde", serde(rename_all = "camelCase"))]
pub struct SmartStreamModule {
    pub module: String,
    pub id: Option<String>,
}

impl Display for SmartStreamModule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.module)
    }
}

/// Generic SmartStream Module
#[derive(Debug, Clone, Default, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "use_serde", serde(rename_all = "camelCase"))]
pub struct SmartStreamJoinModule {
    pub module: String,
    pub id: Option<String>,
    pub right: SmartStreamInputRef,
}

impl Display for SmartStreamJoinModule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.module)
    }
}

#[cfg(test)]
mod test {

    use fluvio_stream_model::store::{MetadataStoreObject, memory::MemoryMeta};

    use super::*;

    #[fluvio_future::test]
    async fn validate_smartstream() {
        let smartstreams: LocalStore<SmartStreamSpec, MemoryMeta> = LocalStore::default();
        let topics: LocalStore<TopicSpec, MemoryMeta> = LocalStore::default();
        let modules: LocalStore<SmartModuleSpec, MemoryMeta> = LocalStore::default();

        let smartstream = SmartStreamSpec {
            input: SmartStreamInputRef::Topic(SmartStreamRef::new("test".into())),
            steps: SmartStreamSteps {
                ..Default::default()
            },
        };

        assert!(smartstream
            .validate(&SmartStreamValidationInput {
                topics: &topics,
                smartstreams: &smartstreams,
                modules: &modules,
            })
            .await
            .is_err());

        let topics2: LocalStore<TopicSpec, MemoryMeta> =
            LocalStore::bulk_new(vec![MetadataStoreObject::with_spec(
                "test",
                TopicSpec::default(),
            )]);

        assert!(smartstream
            .validate(&SmartStreamValidationInput {
                topics: &topics2,
                smartstreams: &smartstreams,
                modules: &modules,
            })
            .await
            .is_ok());
    }

    #[fluvio_future::test]
    async fn validate_smartstream_steps() {
        let smartstreams: LocalStore<SmartStreamSpec, MemoryMeta> = LocalStore::default();
        let modules: LocalStore<SmartModuleSpec, MemoryMeta> = LocalStore::default();

        let smartstream = SmartStreamSpec {
            input: SmartStreamInputRef::Topic(SmartStreamRef::new("test".into())),
            steps: SmartStreamSteps {
                steps: vec![SmartStreamStep::Filter(SmartStreamModule {
                    module: "module1".into(),
                    ..Default::default()
                })],
            },
        };

        let topics: LocalStore<TopicSpec, MemoryMeta> =
            LocalStore::bulk_new(vec![MetadataStoreObject::with_spec(
                "test",
                TopicSpec::default(),
            )]);

        assert!(smartstream
            .validate(&SmartStreamValidationInput {
                topics: &topics,
                smartstreams: &smartstreams,
                modules: &modules,
            })
            .await
            .is_err());

        let modules2: LocalStore<SmartModuleSpec, MemoryMeta> =
            LocalStore::bulk_new(vec![MetadataStoreObject::with_spec(
                "module1",
                SmartModuleSpec::default(),
            )]);

        assert!(smartstream
            .validate(&SmartStreamValidationInput {
                topics: &topics,
                smartstreams: &smartstreams,
                modules: &modules2,
            })
            .await
            .is_ok());
    }
}
