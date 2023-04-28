use minijinja::{Environment};

use crate::{
    secret::{self},
};

use self::{
    context::{Context, ContextStore},
    syntax::ConnectorTemplateSyntax,
};

mod context;
mod syntax;

/// Config renderer. This is the main entry point for rendering a config.
/// It is responsible for resolving secrets and rendering the config.
pub(crate) struct ConfigRenderer {
    inner_renderer: Environment<'static>,
    context_stores: Vec<Box<dyn ContextStore>>,
}

impl ConfigRenderer {
    fn new(
        inner_renderer: Environment<'static>,
        context_stores: Vec<Box<dyn ContextStore>>,
    ) -> Self {
        Self {
            inner_renderer,
            context_stores,
        }
    }

    fn new_with_context_stores(context_stores: Vec<Box<dyn ContextStore>>) -> anyhow::Result<Self> {
        let mut inner_renderer = Environment::new();
        inner_renderer.set_undefined_behavior(minijinja::UndefinedBehavior::Strict);
        inner_renderer.set_syntax(ConnectorTemplateSyntax::default().into())?;
        inner_renderer.set_fuel(Some(500));

        Ok(Self::new(inner_renderer, context_stores))
    }

    fn new_with_default_stores() -> anyhow::Result<Self> {
        let context_stores = Self::default_stores()?;

        Self::new_with_context_stores(context_stores)
    }

    fn render_str(&self, input: &str) -> anyhow::Result<String> {
        let context = self.build_context(input)?;
        match self.inner_renderer.render_str(input, context) {
            Ok(rendered) => Ok(rendered),
            Err(_) => Err(anyhow::anyhow!("failed to render: `{}`.", input)),
        }
    }

    fn default_stores() -> anyhow::Result<Vec<Box<dyn ContextStore>>> {
        Ok(vec![Box::new(secret::default_secret_store()?)])
    }

    fn build_context(&self, input: &str) -> anyhow::Result<Context> {
        let mut context = Context::default();
        for store in self.context_stores.iter() {
            store.add_to_context(&mut context, input)?;
        }
        Ok(context)
    }
}

/// Render a config from a string.
/// This is the main entry point for rendering a config.
/// It is responsible for resolving secrets and rendering the config.
pub fn render_config_str(input: &str) -> anyhow::Result<String> {
    let renderer = ConfigRenderer::new_with_default_stores()?;

    let value = renderer.render_str(input)?;

    Ok(value)
}

#[cfg(test)]
mod test {
    use std::{io::Write};

    use minijinja::value::Value;

    use crate::{
        secret::{FileSecretStore},
        config::{ConnectorConfig},
    };

    use super::{ConfigRenderer, context::ContextStore};

    // mock secret store
    pub struct SecretTestStore;

    impl ContextStore for SecretTestStore {
        fn extract_context_values(&self, _input: &str) -> anyhow::Result<Value> {
            let values = [
                ("foo", "bar"),
                ("api_key", "my_api_key"),
                ("interval", "10s"),
            ]
            .into_iter()
            .collect();
            Ok(values)
        }

        fn context_name(&self) -> &'static str {
            "test_secrets"
        }
    }

    #[test]
    fn test_build_context() {
        let store = Box::new(SecretTestStore);

        let renderer = ConfigRenderer::new_with_context_stores(vec![store]).unwrap();
        let context: super::context::Context = renderer.build_context("").unwrap();

        assert_eq!(
            context
                .0
                .get("test_secrets")
                .unwrap()
                .get_attr("foo")
                .unwrap(),
            "bar".into()
        );
        assert_eq!(context.0.get("test_secrets").unwrap().len().unwrap(), 3);
    }

    #[test]
    fn test_render_str() {
        let store = Box::new(SecretTestStore);
        let renderer = ConfigRenderer::new_with_context_stores(vec![store]).unwrap();
        let output = renderer
            .render_str("hello ${{ test_secrets.foo }}")
            .unwrap();
        assert_eq!(output, "hello bar");
    }

    #[test]
    fn test_render_str_undefined_secret() {
        let store = Box::new(SecretTestStore);

        let renderer = ConfigRenderer::new_with_context_stores(vec![store]).unwrap();
        let output = renderer.render_str("hello ${{ test_secrets.undefined_var }}");
        assert!(output.is_err());
    }

    #[test]
    fn test_invalid_syntax() {
        let store = Box::new(SecretTestStore);

        let renderer = ConfigRenderer::new_with_context_stores(vec![store]).unwrap();
        let output = renderer.render_str("hello ${{");
        assert!(output.is_err());
    }

    #[test]
    fn test_undefined_context_variable() {
        let store = Box::new(SecretTestStore);

        let renderer = ConfigRenderer::new_with_context_stores(vec![store]).unwrap();
        let output = renderer.render_str("hello ${{ some_undefined }}");
        assert!(output.is_err());
    }

    #[test]
    fn test_resolve_config() {
        use super::*;
        let mut file = tempfile::NamedTempFile::new().expect("failed to create tmp file");
        file.write_all(b"foo=bar\napi_key=my_api_key\ninterval=10s")
            .expect("file to write");
        let _ = secret::set_default_secret_store(::std::boxed::Box::new(FileSecretStore::from(
            file.path(),
        )));

        let value_str = r#"
            meta:
                name: test
                version: 0.1.0
                topic: test
                type: http-source
                consumer:
                    partition: 0
                secrets:
                    - name: api_key
                    - name: interval
            my_service:
                api_key: ${{secrets.api_key}}
                interval: ${{ secrets.interval }}
            "#;

        let value_str = render_config_str(value_str).unwrap();
        let connector_config: ConnectorConfig =
            serde_yaml::from_str(&value_str).expect("should be yaml");
        let value: serde_yaml::Value = serde_yaml::from_str(&value_str).expect("should be yaml");

        assert_eq!(connector_config.meta().name, "test");
        assert_eq!(connector_config.meta().version, "0.1.0");
        assert_eq!(connector_config.meta().topic, "test");
        assert_eq!(connector_config.meta().type_, "http-source");
        assert_eq!(
            value["my_service"]["api_key"]
                .as_str()
                .expect("should be str"),
            "my_api_key"
        );
        assert_eq!(
            value["my_service"]["interval"]
                .as_str()
                .expect("should be str"),
            "10s"
        );
    }
}
