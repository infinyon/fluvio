#[cfg(feature = "k8")]
pub mod k8;
#[cfg(feature = "local")]
pub mod local;

use anyhow::Result;
use async_trait::async_trait;
use futures_util::stream::BoxStream;
use fluvio_stream_model::{
    store::{
        MetadataStoreList, k8::K8ExtendedSpec, MetadataStoreObject, actions::LSUpdate, NameSpace,
    },
    core::{Spec, MetadataItem},
};

pub type SharedClient<C> = std::sync::Arc<C>;

#[async_trait]
pub trait MetadataClient<M: MetadataItem>: Send + Sync {
    async fn retrieve_items<S>(&self, namespace: &NameSpace) -> Result<MetadataStoreList<S, M>>
    where
        S: K8ExtendedSpec;

    async fn delete_item<S>(&self, metadata: M) -> Result<()>
    where
        S: K8ExtendedSpec;

    async fn finalize_delete_item<S>(&self, metadata: M) -> Result<()>
    where
        S: K8ExtendedSpec;

    async fn apply<S>(&self, value: MetadataStoreObject<S, M>) -> Result<()>
    where
        S: K8ExtendedSpec,
        <S as Spec>::Owner: K8ExtendedSpec;

    async fn update_spec<S>(&self, metadata: M, spec: S) -> Result<()>
    where
        S: K8ExtendedSpec;

    async fn update_spec_by_key<S>(
        &self,
        key: S::IndexKey,
        namespace: &NameSpace,
        spec: S,
    ) -> Result<()>
    where
        S: K8ExtendedSpec;

    async fn update_status<S>(
        &self,
        metadata: M,
        status: S::Status,
        namespace: &NameSpace,
    ) -> Result<MetadataStoreObject<S, M>>
    where
        S: K8ExtendedSpec;

    fn watch_stream_since<S>(
        &self,
        namespace: &NameSpace,
        resource_version: Option<String>,
    ) -> BoxStream<'_, Result<Vec<LSUpdate<S, M>>>>
    where
        S: K8ExtendedSpec;
}

#[cfg(test)]
mod fixture {
    use std::fmt::Display;

    use fluvio_stream_model::{
        core::{Spec, Status},
        k8_types::{Status as K8Status, DefaultHeader, Crd, CrdNames, Spec as K8Spec},
        store::{
            k8::{K8ExtendedSpec, K8ConvertError, K8MetaItem, default_convert_from_k8},
            MetadataStoreObject,
        },
    };
    use serde::{Serialize, Deserialize};

    #[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub(crate) struct TestSpec {
        pub replica: usize,
    }

    #[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub(crate) struct TestStatus(pub String);

    impl Spec for TestSpec {
        const LABEL: &'static str = "TEST_SPEC";
        type Status = TestStatus;
        type Owner = Self;
        type IndexKey = String;
    }

    impl Status for TestStatus {}

    impl K8ExtendedSpec for TestSpec {
        type K8Spec = TestK8Spec;

        const FINALIZER: Option<&'static str> = Some("FINALIZER1");

        fn convert_from_k8(
            k8_obj: fluvio_stream_model::k8_types::K8Obj<Self::K8Spec>,
            multi_namespace_context: bool,
        ) -> std::result::Result<MetadataStoreObject<Self, K8MetaItem>, K8ConvertError<Self::K8Spec>>
        {
            default_convert_from_k8(k8_obj, multi_namespace_context)
        }

        fn convert_status_from_k8(status: Self::Status) -> <Self::K8Spec as K8Spec>::Status {
            TestK8SpecStatus(status.0)
        }

        fn into_k8(self) -> Self::K8Spec {
            TestK8Spec {
                replica: self.replica,
            }
        }
    }

    impl Display for TestStatus {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    #[derive(Debug, Default, Clone, PartialEq, Eq, Serialize, Deserialize)]
    pub(crate) struct TestK8Spec {
        pub replica: usize,
    }

    #[derive(Default, Debug, Eq, PartialEq, Clone, Serialize, Deserialize)]
    pub(crate) struct TestK8SpecStatus(String);

    impl K8Status for TestK8SpecStatus {}

    impl Display for TestK8SpecStatus {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            write!(f, "{}", self.0)
        }
    }

    impl K8Spec for TestK8Spec {
        type Status = TestK8SpecStatus;
        type Header = DefaultHeader;

        fn metadata() -> &'static Crd {
            &Crd {
                group: "test.fluvio",
                version: "v1",
                names: CrdNames {
                    kind: "myspec",
                    plural: "myspecs",
                    singular: "myspec",
                },
            }
        }
    }

    impl From<TestK8Spec> for TestSpec {
        fn from(value: TestK8Spec) -> Self {
            Self {
                replica: value.replica,
            }
        }
    }

    impl From<TestK8SpecStatus> for TestStatus {
        fn from(value: TestK8SpecStatus) -> Self {
            Self(value.0)
        }
    }
}
