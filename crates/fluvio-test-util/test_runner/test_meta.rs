use crate::test_meta::{TestCase, TestOption};
use crate::test_meta::test_result::TestResult;
use super::test_driver::TestDriver;
use crate::test_meta::derive_attr::TestRequirements;
use crate::test_meta::environment::EnvDetail;

#[derive(Debug)]
pub struct FluvioTestMeta {
    pub name: &'static str,
    pub test_fn: fn(TestDriver, TestCase) -> Result<TestResult, TestResult>,
    pub validate_fn: fn(Vec<String>) -> Box<dyn TestOption>,
    pub requirements: fn() -> TestRequirements,
}

inventory::collect!(FluvioTestMeta);
impl FluvioTestMeta {
    pub fn all_test_names() -> Vec<&'static str> {
        inventory::iter::<Self>
            .into_iter()
            .map(|x| x.name)
            .collect::<Vec<&str>>()
    }

    pub fn from_name(test_name: String) -> Option<&'static Self> {
        inventory::iter::<Self>
            .into_iter()
            .find(|t| t.name == test_name)
    }

    pub fn set_topic(test_reqs: &TestRequirements, test_case: &mut TestCase) {
        // If the topic name is given over CLI, that value should override
        if test_case.environment.is_topic_set() {
            test_case
                .environment
                .set_base_topic_name(test_case.environment.base_topic_name())
        // Next, we should fall back to the value set on the macro
        } else if let Some(topic) = &test_reqs.topic {
            test_case.environment.set_base_topic_name(topic.to_string());
        } else {
            test_case
                .environment
                .set_base_topic_name("topic".to_string());
        }
    }

    pub fn set_timeout(test_reqs: &TestRequirements, test_case: &mut TestCase) {
        // If timeout is set in #[fluvio_test()] macro
        if let Some(timeout) = test_reqs.timeout {
            test_case.environment.set_timeout(timeout)
        } else {
            // Default CLI value
            test_case
                .environment
                .set_timeout(test_case.environment.timeout)
        }
    }

    pub fn customize_test(test_reqs: &TestRequirements, test_case: &mut TestCase) {
        Self::set_topic(test_reqs, test_case);
        Self::set_timeout(test_reqs, test_case);
    }
}
