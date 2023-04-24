pub use fluvio_connector_package::config::ConnectorConfig;
use tracing::trace;

use std::{path::PathBuf, fs::File};

use serde::de::DeserializeOwned;
use anyhow::{Result, Context};
use serde_yaml::Value;

pub fn value_from_file<P: Into<PathBuf>>(path: P) -> Result<Value> {
    let file = File::open(path.into())?;
    serde_yaml::from_reader(file).context("unable to parse config file into YAML")
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
