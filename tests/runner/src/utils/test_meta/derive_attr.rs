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
    Benchmark(bool),
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
                "benchmark" => Self::benchmark(&name_value),
                _ => Err(SynError::new(name_value.span(), "Unsupported key")),
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

    fn benchmark(name_value: &syn::MetaNameValue) -> Result<TestRequirementAttribute, SynError> {
        if let Lit::Bool(b) = &name_value.lit {
            Ok(Self::Benchmark(b.value()))
        } else {
            Err(SynError::new(
                name_value.span(),
                "Benchmark must be a LitBool",
            ))
        }
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
        if let Lit::Int(timeout) = &name_value.lit {
            let parsed = u64::from_str_radix(timeout.base10_digits(), 10).expect("Parse failed");
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
                u16::from_str_radix(min_spu.base10_digits(), 10).expect("Parse failed"),
            ))
        } else {
            Err(SynError::new(name_value.span(), "Min spu must be LitInt"))
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
    pub benchmark: Option<bool>,
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
                    test_requirements.timeout = Some(timeout)
                }
                TestRequirementAttribute::ClusterType(cluster_type) => {
                    test_requirements.cluster_type = Some(cluster_type)
                }
                TestRequirementAttribute::TestName(name) => {
                    test_requirements.test_name = Some(name)
                }
                TestRequirementAttribute::Benchmark(is_benchmark) => {
                    test_requirements.benchmark = Some(is_benchmark)
                }
            }
        }

        test_requirements
    }
}
