use std::io::Read;

use anyhow::{Result, Context};
use serde::de::DeserializeOwned;
use serde_yaml::Value;
use tracing::trace;

pub use fluvio_connector_package::config::ConnectorConfig;

pub fn value_from_reader<R: Read>(reader: R) -> Result<Value> {
    serde_yaml::from_reader(reader).context("unable to parse config file into YAML")
}

pub fn from_value<T: DeserializeOwned>(value: Value, root: Option<&str>) -> Result<T> {
    let value = get_value(value, root)?;
    serde_yaml::from_value(value).context("unable to parse custom config type from YAML")
}

pub fn get_value(value: Value, root: Option<&str>) -> Result<Value> {
    let value = match root {
        Some(root) => value
            .get(root)
            .cloned()
            .unwrap_or(Value::Mapping(Default::default())),
        None => value,
    };
    trace!("{:#?}", value);
    Ok(value)
}

#[cfg(test)]
mod tests {
    use fluvio_connector_package::config::MetaConfig;

    #[test]
    fn test_from_value() {
        use super::*;
        let value = serde_yaml::from_str(
            r#"
        meta:
            name: test
            version: 0.1.0
            topic: test
            type: http-source
            consumer:
                partition: 0
        http:
            port: 8080
        "#,
        )
        .unwrap();
        let config: MetaConfig = from_value(value, Some("meta")).unwrap();
        assert_eq!(config.name, "test");
        assert_eq!(config.topic, "test");
        assert_eq!(
            config
                .consumer
                .expect("expected some consume config")
                .partition,
            Some(0)
        );
    }
}
