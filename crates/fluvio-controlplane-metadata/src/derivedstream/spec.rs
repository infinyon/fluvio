//!
//! # DerivedStream Spec
//!
//!

use std::fmt::Display;
use std::marker::PhantomData;

use tracing::trace;

use fluvio_protocol::{Encoder, Decoder};
use fluvio_stream_model::core::Spec;
use fluvio_stream_model::{core::MetadataItem, store::LocalStore};

use crate::smartmodule::{SmartModuleSpec};
use crate::topic::TopicSpec;

use super::metadata::DerivedStreamValidationError;

/// DerivedStream is unstable feature
#[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct DerivedStreamSpec {
    pub input: DerivedStreamInputRef,
    #[cfg_attr(feature = "use_serde", serde(flatten))]
    pub steps: DerivedStreamSteps,
}

impl DerivedStreamSpec {
    // validat configuration
    pub async fn validate<'a, C>(
        &'a self,
        objects: &DerivedStreamValidationInput<'a, C>,
    ) -> Result<(), DerivedStreamValidationError>
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
pub enum DerivedStreamInputRef {
    #[cfg_attr(feature = "use_serde", serde(rename = "topic"))]
    Topic(DerivedStreamRef<TopicSpec>),
    #[cfg_attr(feature = "use_serde", serde(rename = "derivedstream"))]
    DerivedStream(DerivedStreamRef<DerivedStreamSpec>),
}

impl Default for DerivedStreamInputRef {
    fn default() -> Self {
        DerivedStreamInputRef::Topic(DerivedStreamRef::default())
    }
}

impl Display for DerivedStreamInputRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DerivedStreamInputRef::Topic(ref topic) => write!(f, "Topic({})", topic),
            DerivedStreamInputRef::DerivedStream(ref stream) => {
                write!(f, "DerivedStream({})", stream)
            }
        }
    }
}

impl DerivedStreamInputRef {
    // validat configuration
    pub async fn validate<'a, C>(
        &'a self,
        objects: &DerivedStreamValidationInput<'a, C>,
    ) -> Result<(), DerivedStreamValidationError>
    where
        C: MetadataItem,
    {
        match self {
            DerivedStreamInputRef::Topic(ref topic_ref) => {
                if !topic_ref.validate(objects.topics).await {
                    trace!(topic = %topic_ref.name,"topic not found");
                    return Err(DerivedStreamValidationError::TopicNotFound(
                        topic_ref.name.clone(),
                    ));
                }
            }
            DerivedStreamInputRef::DerivedStream(ref smart_stream_ref) => {
                if !smart_stream_ref.validate(objects.derivedstreams).await {
                    return Err(DerivedStreamValidationError::DerivedStreamNotFound(
                        smart_stream_ref.name.clone(),
                    ));
                }
            }
        }
        Ok(())
    }
}

#[derive(Debug, Default, Clone, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct DerivedStreamRef<S>
where
    S: Spec + Default + Encoder + Decoder,
    S::IndexKey: Default + Encoder + Decoder,
{
    pub name: S::IndexKey,
    #[cfg_attr(feature = "use_serde", serde(skip))]
    data: PhantomData<S>,
}

impl<S> Display for DerivedStreamRef<S>
where
    S: Spec + Default + Encoder + Decoder,
    S::IndexKey: Default + Encoder + Decoder + Display,
{
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name)
    }
}

impl<S> DerivedStreamRef<S>
where
    S: Spec + Default + Encoder + Decoder,
    S::IndexKey: Default + Encoder + Decoder,
{
    pub fn new(name: S::IndexKey) -> Self {
        DerivedStreamRef {
            name,
            data: PhantomData,
        }
    }
}

impl<S> DerivedStreamRef<S>
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

pub struct DerivedStreamValidationInput<'a, C>
where
    C: MetadataItem,
{
    pub topics: &'a LocalStore<TopicSpec, C>,
    pub derivedstreams: &'a LocalStore<DerivedStreamSpec, C>,
    pub modules: &'a LocalStore<SmartModuleSpec, C>,
}

#[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(
    feature = "use_serde",
    derive(serde::Serialize, serde::Deserialize),
    serde(rename_all = "camelCase")
)]
pub struct DerivedStreamSteps {
    pub steps: Vec<DerivedStreamStep>,
}

impl Display for DerivedStreamSteps {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let modules: String = self.steps.iter().map(|t| t.to_string()).collect();
        write!(f, "{}", modules)
    }
}

impl DerivedStreamSteps {
    async fn validate<'a, C>(
        &'a self,
        modules: &'a LocalStore<SmartModuleSpec, C>,
    ) -> Result<(), DerivedStreamValidationError>
    where
        C: MetadataItem,
    {
        for step in &self.steps {
            let module = step.module();
            if !modules.contains_key(module).await {
                return Err(DerivedStreamValidationError::SmartModuleNotFound(
                    module.to_string(),
                ));
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum DerivedStreamStep {
    #[cfg_attr(feature = "use_serde", serde(rename = "filter"))]
    Filter(DerivedStreamModule),
    #[cfg_attr(feature = "use_serde", serde(rename = "map"))]
    Map(DerivedStreamModule),
    #[cfg_attr(feature = "use_serde", serde(rename = "filterMap"))]
    FilterMap(DerivedStreamModule),
    #[cfg_attr(feature = "use_serde", serde(rename = "aggregate"))]
    Aggregate(DerivedStreamModule),
    #[cfg_attr(feature = "use_serde", serde(rename = "join"))]
    Join(DerivedStreamJoinModule),
}

impl Default for DerivedStreamStep {
    fn default() -> Self {
        DerivedStreamStep::Filter(DerivedStreamModule::default())
    }
}

impl Display for DerivedStreamStep {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DerivedStreamStep::Filter(module) => write!(f, "Filter({})", module),
            DerivedStreamStep::Map(module) => write!(f, "Map({})", module),
            DerivedStreamStep::FilterMap(module) => write!(f, "FilterMap({})", module),
            DerivedStreamStep::Aggregate(module) => write!(f, "Aggregate({})", module),
            DerivedStreamStep::Join(module) => write!(f, "Join({})", module),
        }
    }
}

impl DerivedStreamStep {
    pub fn module(&self) -> &str {
        match self {
            DerivedStreamStep::Filter(ref module) => &module.module,
            DerivedStreamStep::Map(ref module) => &module.module,
            DerivedStreamStep::FilterMap(ref module) => &module.module,
            DerivedStreamStep::Aggregate(ref module) => &module.module,
            DerivedStreamStep::Join(ref module) => &module.module,
        }
    }
}

/// Generic DerivedStream Module
#[derive(Debug, Clone, Default, Eq, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "use_serde", serde(rename_all = "camelCase"))]
pub struct DerivedStreamModule {
    pub module: String,
    pub id: Option<String>,
}

impl Display for DerivedStreamModule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.module)
    }
}

/// Generic DerivedStream Module
#[derive(Debug, Clone, Default, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
#[cfg_attr(feature = "use_serde", serde(rename_all = "camelCase"))]
pub struct DerivedStreamJoinModule {
    pub module: String,
    pub id: Option<String>,
    pub right: DerivedStreamInputRef,
}

impl Display for DerivedStreamJoinModule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.module)
    }
}

#[cfg(test)]
mod test {

    use fluvio_stream_model::store::{MetadataStoreObject, memory::MemoryMeta};

    use super::*;

    #[fluvio_future::test]
    async fn validate_derivedstream() {
        let derivedstreams: LocalStore<DerivedStreamSpec, MemoryMeta> = LocalStore::default();
        let topics: LocalStore<TopicSpec, MemoryMeta> = LocalStore::default();
        let modules: LocalStore<SmartModuleSpec, MemoryMeta> = LocalStore::default();

        let derivedstream = DerivedStreamSpec {
            input: DerivedStreamInputRef::Topic(DerivedStreamRef::new("test".into())),
            steps: DerivedStreamSteps {
                ..Default::default()
            },
        };

        assert!(derivedstream
            .validate(&DerivedStreamValidationInput {
                topics: &topics,
                derivedstreams: &derivedstreams,
                modules: &modules,
            })
            .await
            .is_err());

        let topics2: LocalStore<TopicSpec, MemoryMeta> =
            LocalStore::bulk_new(vec![MetadataStoreObject::with_spec(
                "test",
                TopicSpec::default(),
            )]);

        assert!(derivedstream
            .validate(&DerivedStreamValidationInput {
                topics: &topics2,
                derivedstreams: &derivedstreams,
                modules: &modules,
            })
            .await
            .is_ok());
    }

    #[fluvio_future::test]
    async fn validate_derivedstream_steps() {
        let derivedstreams: LocalStore<DerivedStreamSpec, MemoryMeta> = LocalStore::default();
        let modules: LocalStore<SmartModuleSpec, MemoryMeta> = LocalStore::default();

        let derivedstream = DerivedStreamSpec {
            input: DerivedStreamInputRef::Topic(DerivedStreamRef::new("test".into())),
            steps: DerivedStreamSteps {
                steps: vec![DerivedStreamStep::Filter(DerivedStreamModule {
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

        assert!(derivedstream
            .validate(&DerivedStreamValidationInput {
                topics: &topics,
                derivedstreams: &derivedstreams,
                modules: &modules,
            })
            .await
            .is_err());

        let modules2: LocalStore<SmartModuleSpec, MemoryMeta> =
            LocalStore::bulk_new(vec![MetadataStoreObject::with_spec(
                "module1",
                SmartModuleSpec::default(),
            )]);

        assert!(derivedstream
            .validate(&DerivedStreamValidationInput {
                topics: &topics,
                derivedstreams: &derivedstreams,
                modules: &modules2,
            })
            .await
            .is_ok());
    }
}
