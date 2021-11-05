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

pub type SmartStreamModuleRef = SmartStreamRef<SmartModuleSpec>;

/// SmartStream is unstable feature
#[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartStreamSpec {
    pub inputs: SmartStreamInputs,
    pub modules: SmartStreamModules,
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
        self.inputs.validate(objects).await?;
        trace!("validating output");
        self.modules.validate(objects.modules).await?;
        Ok(())
    }
}

#[derive(Debug, Default, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub struct SmartStreamInputs {
    pub left: SmartStreamInput,
    pub right: Option<SmartStreamInput>,
}

impl SmartStreamInputs {
    pub fn right_display(&self) -> String {
        match self.right {
            Some(ref right) => right.to_string(),
            None => "".to_string(),
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
        self.left.validate(objects).await?;
        if let Some(right) = &self.right {
            right.validate(objects).await?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Encoder, Decoder)]
#[cfg_attr(feature = "use_serde", derive(serde::Serialize, serde::Deserialize))]
pub enum SmartStreamInput {
    #[cfg_attr(feature = "use_serde", serde(rename = "topic"))]
    Topic(SmartStreamRef<TopicSpec>),
    #[cfg_attr(feature = "use_serde", serde(rename = "smartstream"))]
    SmartStream(SmartStreamRef<SmartStreamSpec>),
}

impl Default for SmartStreamInput {
    fn default() -> Self {
        SmartStreamInput::Topic(SmartStreamRef::default())
    }
}

impl Display for SmartStreamInput {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SmartStreamInput::Topic(ref topic) => write!(f, "Topic({})", topic),
            SmartStreamInput::SmartStream(ref stream) => write!(f, "SmartStream({})", stream),
        }
    }
}

impl SmartStreamInput {
    // validat configuration
    pub async fn validate<'a, C>(
        &'a self,
        objects: &SmartStreamValidationInput<'a, C>,
    ) -> Result<(), SmartStreamValidationError>
    where
        C: MetadataItem,
    {
        match self {
            SmartStreamInput::Topic(ref topic_ref) => {
                if !topic_ref.validate(objects.topics).await {
                    trace!(topic = %topic_ref.name,"topic not found");
                    return Err(SmartStreamValidationError::TopicNotFound(
                        topic_ref.name.clone(),
                    ));
                }
            }
            SmartStreamInput::SmartStream(ref smart_stream_ref) => {
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
pub struct SmartStreamModules {
    pub transforms: Vec<SmartStreamRef<SmartModuleSpec>>,
    pub outputs: Vec<SmartStreamRef<SmartModuleSpec>>,
}

impl Display for SmartStreamModules {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let modules: String = self.transforms.iter().map(|t| t.to_string()).collect();
        write!(f, "{}", modules)
    }
}

impl SmartStreamModules {
    async fn validate<'a, C>(
        &'a self,
        modules: &'a LocalStore<SmartModuleSpec, C>,
    ) -> Result<(), SmartStreamValidationError>
    where
        C: MetadataItem,
    {
        for transform in &self.transforms {
            if !transform.validate(modules).await {
                return Err(SmartStreamValidationError::SmartModuleNotFound(
                    transform.name.clone(),
                ));
            }
        }
        for output in &self.outputs {
            if output.validate(modules).await {
                return Err(SmartStreamValidationError::SmartModuleNotFound(
                    output.name.clone(),
                ));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod test {

    use fluvio_stream_model::store::{MetadataStoreObject, memory::MemoryMeta};

    use super::*;

    /*
    #[test]
    fn test_smartstream_spec_deserialiation() {
        let _spec: SmartStreamInputs = serde_json::from_str(
            r#"
                {
                    "left": {
                        "topic": {
                            "name": "test"
                        }
                    }
                }
            "#,
        )
        .expect("spec");
    }
    */

    #[fluvio_future::test]
    async fn validate_smartstream() {
        let smartstreams: LocalStore<SmartStreamSpec, MemoryMeta> = LocalStore::default();
        let topics: LocalStore<TopicSpec, MemoryMeta> = LocalStore::default();
        let modules: LocalStore<SmartModuleSpec, MemoryMeta> = LocalStore::default();

        let smartstream = SmartStreamSpec {
            inputs: SmartStreamInputs {
                left: SmartStreamInput::Topic(SmartStreamRef::new("test".into())),
                right: None,
            },
            modules: SmartStreamModules {
                transforms: vec![],
                outputs: vec![],
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
}
