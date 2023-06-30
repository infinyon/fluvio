use std::time::Duration;

use serde::{Serialize, Deserialize};
use syn::{Error as SynError, Result as SynResult, LitBool};
use syn::meta::ParseNestedMeta;

use syn::spanned::Spanned;

use crate::setup::environment::EnvironmentType;

#[derive(Debug)]
pub enum TestRequirementAttribute {
    MinSpu(u16),
    Topic(String),
    Timeout(Duration),
    ClusterType(EnvironmentType),
    TestName(String),
    Async(bool),
}

impl TestRequirementAttribute {
    fn from_ast(meta: ParseNestedMeta) -> SynResult<Self> {
        if meta.path.is_ident("min_spu") {
            Self::min_spu(meta.value()?)
        } else if meta.path.is_ident("topic") {
            Self::topic(meta.value()?)
        } else if meta.path.is_ident("timeout") {
            Self::timeout(meta.value()?)
        } else if meta.path.is_ident("cluster_type") {
            Self::cluster_type(meta.value()?)
        } else if meta.path.is_ident("name") {
            Self::name(meta.value()?)
        } else if meta.path.is_ident("async") {
            match meta.value() {
                Ok(buffer) => Self::async_nv(buffer),
                Err(_) => Ok(Self::Async(true)),
            }
        } else {
            Err(SynError::new(meta.path.span(), "Unsupported attribute:"))
        }
    }

    fn name(name_value: &syn::parse::ParseBuffer) -> Result<TestRequirementAttribute, SynError> {
        if let Ok(lit_str) = name_value.parse::<syn::LitStr>() {
            return Ok(Self::TestName(lit_str.value()));
        }

        Err(SynError::new(
            name_value.span(),
            "TestName must be a LitStr",
        ))
    }

    fn cluster_type(
        name_value: &syn::parse::ParseBuffer,
    ) -> Result<TestRequirementAttribute, SynError> {
        if let Ok(lit_str) = name_value.parse::<syn::LitStr>() {
            if lit_str.value().to_lowercase() == "k8" {
                return Ok(Self::ClusterType(EnvironmentType::K8));
            } else if lit_str.value().to_lowercase() == "local" {
                return Ok(Self::ClusterType(EnvironmentType::Local));
            } else {
                return  Err(SynError::new(
                    name_value.span(),
                    "ClusterType values must be \"k8\" or \"local\". Don't define cluster_type if both.",
                ));
            }
        }
        Err(SynError::new(
            name_value.span(),
            "ClusterType must be a LitStr",
        ))
    }

    fn timeout(name_value: &syn::parse::ParseBuffer) -> Result<TestRequirementAttribute, SynError> {
        if let Ok(lit_bool) = name_value.parse::<syn::LitBool>() {
            if lit_bool.value() {
                return Err(SynError::new(
                    lit_bool.span(),
                    "timeout must be u64 or false",
                ));
            } else {
                return Ok(Self::Timeout(Duration::MAX));
            }
        } else if let Ok(timeout) = &name_value.parse::<syn::LitInt>() {
            let parsed = timeout
                .base10_digits()
                .parse::<u64>()
                .expect("Parse failed");
            return Ok(Self::Timeout(Duration::from_secs(parsed)));
        }

        Err(SynError::new(name_value.span(), "Timeout must be LitInt"))
    }

    fn topic(name_value: &syn::parse::ParseBuffer) -> Result<TestRequirementAttribute, SynError> {
        if let Ok(lit_str) = name_value.parse::<syn::LitStr>() {
            return Ok(Self::Topic(lit_str.value()));
        }
        Err(SynError::new(name_value.span(), "Topic must be a LitStr"))
    }

    fn min_spu(name_value: &syn::parse::ParseBuffer) -> Result<TestRequirementAttribute, SynError> {
        if let Ok(lit_int) = name_value.parse::<syn::LitInt>() {
            return Ok(Self::MinSpu(
                lit_int
                    .base10_digits()
                    .parse::<u16>()
                    .expect("Parse failed"),
            ));
        }
        Err(SynError::new(name_value.span(), "Min spu must be LitInt"))
    }

    fn async_nv(
        name_value: &syn::parse::ParseBuffer,
    ) -> Result<TestRequirementAttribute, SynError> {
        if let Ok(Some(lit_bool)) = name_value.parse::<Option<LitBool>>() {
            return Ok(Self::Async(lit_bool.value()));
        }
        Err(SynError::new(
            name_value.span(),
            "Async must be LitBool or have no key",
        ))
    }
}

// These are the arguments used by `#[fluvio_test()]
#[derive(Debug, Default, Serialize, Deserialize)]
pub struct TestRequirements {
    pub min_spu: Option<u16>,
    pub topic: Option<String>,
    pub timeout: Option<Duration>,
    pub cluster_type: Option<EnvironmentType>,
    pub test_name: Option<String>,
    pub r#async: bool,
}

impl TestRequirements {
    pub fn parse(&mut self, meta: ParseNestedMeta) -> SynResult<()> {
        let attr = TestRequirementAttribute::from_ast(meta)?;
        self.set_attr(attr);

        Ok(())
    }

    fn set_attr(&mut self, attr: TestRequirementAttribute) {
        match attr {
            TestRequirementAttribute::MinSpu(min_spu) => self.min_spu = Some(min_spu),
            TestRequirementAttribute::Topic(topic) => self.topic = Some(topic),
            TestRequirementAttribute::Timeout(timeout) => {
                if timeout.is_zero() {
                    self.timeout = None
                } else {
                    self.timeout = Some(timeout)
                }
            }
            TestRequirementAttribute::ClusterType(cluster_type) => {
                self.cluster_type = Some(cluster_type)
            }
            TestRequirementAttribute::TestName(name) => self.test_name = Some(name),
            TestRequirementAttribute::Async(is_async) => self.r#async = is_async,
        }
    }
}
