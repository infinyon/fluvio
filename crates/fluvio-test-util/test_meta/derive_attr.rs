use serde::{Serialize, Deserialize};
use syn::{AttributeArgs, Error as SynError, Lit, Meta, NestedMeta, Path, Result as SynResult};
use syn::spanned::Spanned;
use crate::setup::environment::EnvironmentType;
use std::time::Duration;

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
    fn from_ast(meta: Meta) -> SynResult<Self> {
        match meta {
            Meta::NameValue(name_value) => match Self::get_key(&name_value.path).as_str() {
                "min_spu" => Self::min_spu(&name_value),
                "topic" => Self::topic(&name_value),
                "timeout" => Self::timeout(&name_value),
                "cluster_type" => Self::cluster_type(&name_value),
                "name" => Self::name(&name_value),
                "async" => Self::async_nv(&name_value),
                _ => Err(SynError::new(name_value.span(), "Unsupported key")),
            },
            Meta::Path(path) => match Self::get_key(&path).as_str() {
                "async" => Self::async_path(&path),
                _ => Err(SynError::new(path.span(), "Unsupported key")),
            },
            _ => Err(SynError::new(meta.span(), "Unsupported attribute:")),
        }
    }

    fn get_key(p: &Path) -> String {
        let mut key: Vec<String> = p
            .segments
            .iter()
            .map(|args| args.ident.to_string())
            .collect();

        key.pop().expect("Key expected")
    }

    fn name(name_value: &syn::MetaNameValue) -> Result<TestRequirementAttribute, SynError> {
        if let Lit::Str(str) = &name_value.lit {
            Ok(Self::TestName(str.value()))
        } else {
            Err(SynError::new(
                name_value.span(),
                "TestName must be a LitStr",
            ))
        }
    }

    fn cluster_type(name_value: &syn::MetaNameValue) -> Result<TestRequirementAttribute, SynError> {
        if let Lit::Str(str) = &name_value.lit {
            if str.value().to_lowercase() == "k8" {
                Ok(Self::ClusterType(EnvironmentType::K8))
            } else if str.value().to_lowercase() == "local" {
                Ok(Self::ClusterType(EnvironmentType::Local))
            } else {
                Err(SynError::new(
                name_value.span(),
                "ClusterType values must be \"k8\" or \"local\". Don't define cluster_type if both.",
            ))
            }
        } else {
            Err(SynError::new(
                name_value.span(),
                "ClusterType must be a LitStr",
            ))
        }
    }

    fn timeout(name_value: &syn::MetaNameValue) -> Result<TestRequirementAttribute, SynError> {
        if let Lit::Bool(boot_lit) = &name_value.lit {
            if boot_lit.value() {
                Err(SynError::new(
                    boot_lit.span(),
                    "timeout must be u64 or false",
                ))
            } else {
                Ok(Self::Timeout(Duration::MAX))
            }
        } else if let Lit::Int(timeout) = &name_value.lit {
            let parsed = timeout
                .base10_digits()
                .parse::<u64>()
                .expect("Parse failed");
            Ok(Self::Timeout(Duration::from_secs(parsed)))
        } else {
            Err(SynError::new(name_value.span(), "Timeout must be LitInt"))
        }
    }

    fn topic(name_value: &syn::MetaNameValue) -> Result<TestRequirementAttribute, SynError> {
        if let Lit::Str(str) = &name_value.lit {
            Ok(Self::Topic(str.value()))
        } else {
            Err(SynError::new(name_value.span(), "Topic must be a LitStr"))
        }
    }

    fn min_spu(name_value: &syn::MetaNameValue) -> Result<TestRequirementAttribute, SynError> {
        if let Lit::Int(min_spu) = &name_value.lit {
            Ok(Self::MinSpu(
                min_spu
                    .base10_digits()
                    .parse::<u16>()
                    .expect("Parse failed"),
            ))
        } else {
            Err(SynError::new(name_value.span(), "Min spu must be LitInt"))
        }
    }

    // Support #[fluvio_test(async)] and #[fluvio_test(async = true)] + #[fluvio_test(async = false)]
    fn async_path(_path: &syn::Path) -> Result<TestRequirementAttribute, SynError> {
        Ok(Self::Async(true))
    }

    fn async_nv(name_value: &syn::MetaNameValue) -> Result<TestRequirementAttribute, SynError> {
        if let Lit::Bool(bool_lit) = &name_value.lit {
            Ok(Self::Async(bool_lit.value()))
        } else {
            Err(SynError::new(
                name_value.span(),
                "Async must be LitBool or have no key",
            ))
        }
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
    pub fn from_ast(args: AttributeArgs) -> SynResult<Self> {
        let mut attrs: Vec<TestRequirementAttribute> = Vec::new();

        for attr in args {
            match attr {
                NestedMeta::Meta(meta) => {
                    attrs.push(TestRequirementAttribute::from_ast(meta)?);
                }
                _ => return Err(SynError::new(attr.span(), "invalid syntax")),
            }
        }

        Ok(Self::from(attrs))
    }

    fn from(attrs: Vec<TestRequirementAttribute>) -> Self {
        let mut test_requirements = TestRequirements::default();

        for attr in attrs {
            match attr {
                TestRequirementAttribute::MinSpu(min_spu) => {
                    test_requirements.min_spu = Some(min_spu)
                }
                TestRequirementAttribute::Topic(topic) => test_requirements.topic = Some(topic),
                TestRequirementAttribute::Timeout(timeout) => {
                    if timeout.is_zero() {
                        test_requirements.timeout = None
                    } else {
                        test_requirements.timeout = Some(timeout)
                    }
                }
                TestRequirementAttribute::ClusterType(cluster_type) => {
                    test_requirements.cluster_type = Some(cluster_type)
                }
                TestRequirementAttribute::TestName(name) => {
                    test_requirements.test_name = Some(name)
                }
                TestRequirementAttribute::Async(is_async) => test_requirements.r#async = is_async,
            }
        }

        test_requirements
    }
}
