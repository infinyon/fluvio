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
    let value = match root {
        Some(root) => value
            .get(root)
            .cloned()
            .unwrap_or(Value::Mapping(Default::default())),
        None => value,
    };
    trace!("{:#?}", value);
    serde_yaml::from_value(value).context("unable to parse custom config type from YAML")
}
