use std::sync::Arc;
use crate::test_meta::{TestCase, TestOption};
use crate::test_meta::test_result::TestResult;
use super::test_driver::TestDriver;
use crate::test_meta::derive_attr::TestRequirements;
use crate::test_meta::environment::EnvDetail;

#[derive(Debug)]
pub struct FluvioTestMeta {
    pub name: String,
    pub test_fn: fn(Arc<TestDriver>, TestCase) -> Result<TestResult, TestResult>,
    pub validate_fn: fn(Vec<String>) -> Box<dyn TestOption>,
    pub requirements: fn() -> TestRequirements,
}

inventory::collect!(FluvioTestMeta);

impl FluvioTestMeta {
    pub fn all_test_names() -> Vec<&'static str> {
        inventory::iter::<Self>
            .into_iter()
            .map(|x| x.name.as_str())
            .collect::<Vec<&str>>()
    }

    pub fn from_name<S: AsRef<str>>(test_name: S) -> Option<&'static Self> {
        inventory::iter::<Self>
            .into_iter()
            .find(|t| t.name == test_name.as_ref())
    }

    pub fn set_topic(test_reqs: &TestRequirements, test_case: &mut TestCase) {
        // If the topic name is given over CLI, that value should override
        if test_case.environment.is_topic_set() {
            test_case
                .environment
                .set_topic_name(test_case.environment.topic_name())
        // Next, we should fall back to the value set on the macro
        } else if let Some(topic) = &test_reqs.topic {
            test_case.environment.set_topic_name(topic.to_string());
        } else {
            test_case.environment.set_topic_name("topic".to_string());
        }
    }

    pub fn set_timeout(test_reqs: &TestRequirements, test_case: &mut TestCase) {
        // Set timer
        if let Some(timeout) = test_reqs.timeout {
            test_case.environment.set_timeout(timeout)
        }
    }

    pub fn customize_test(test_reqs: &TestRequirements, test_case: &mut TestCase) {
        Self::set_topic(test_reqs, test_case);
        Self::set_timeout(test_reqs, test_case);
    }
}
