use std::collections::HashMap;

use serde::Serialize;
use minijinja::value::Value;

use crate::{secret::SecretStore, config::ConnectorConfig};

/// Context for the template engine
/// This is the data that is available to the template engine
/// when it is rendering the template.
#[derive(Serialize, Default)]
pub(crate) struct Context(pub(crate) HashMap<&'static str, Value>);

/// Template engine does not allow lazy loading of context values.
/// This trait allows us to load context values on demand.
pub(crate) trait ContextStore {
    /// values to add to the context
    fn extract_context_values(&self, input: &str) -> anyhow::Result<Value>;
    /// key name for the context
    fn context_name(&self) -> &'static str;
}
impl dyn ContextStore {
    pub(crate) fn add_to_context(&self, context: &mut Context, input: &str) -> anyhow::Result<()> {
        let value = self.extract_context_values(input)?;

        context.0.insert(self.context_name(), value);
        Ok(())
    }
}
impl ContextStore for &dyn SecretStore {
    fn extract_context_values(&self, input: &str) -> anyhow::Result<Value> {
        let connector_config: ConnectorConfig = serde_yaml::from_reader(input.as_bytes())?;
        let mut values = HashMap::default();

        for secret in connector_config.secrets().iter() {
            let secret_value = self.read(secret.name())?;
            values.insert(secret.name().to_owned(), secret_value);
        }
        Ok(values.into())
    }

    fn context_name(&self) -> &'static str {
        "secrets"
    }
}

#[cfg(test)]
mod test {

    use minijinja::value::Value;

    use super::ContextStore;

    pub struct TestStore;

    impl ContextStore for TestStore {
        fn extract_context_values(&self, _input: &str) -> anyhow::Result<Value> {
            Ok([("test", "value")].into_iter().collect())
        }

        fn context_name(&self) -> &'static str {
            "test"
        }
    }

    #[test]
    fn test_context_store() {
        let store = &TestStore as &dyn ContextStore;
        let mut context = super::Context::default();
        store.add_to_context(&mut context, "").unwrap();
        assert_eq!(
            context
                .0
                .get("test")
                .unwrap()
                .get_attr("test")
                .unwrap()
                .as_str()
                .unwrap(),
            "value"
        );
    }
}
