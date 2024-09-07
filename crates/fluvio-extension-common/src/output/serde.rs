use std::sync::Arc;

use serde::Serialize;
use clap::ValueEnum;

use super::Terminal;

use super::OutputType;
use super::OutputError;

#[derive(ValueEnum, Debug, Clone, Eq, PartialEq)]
#[allow(non_camel_case_types)]
pub enum SerializeType {
    yaml,
    json,
    toml,
}

impl From<OutputType> for SerializeType {
    fn from(output: OutputType) -> Self {
        match output {
            OutputType::yaml => SerializeType::yaml,
            OutputType::json => SerializeType::json,
            OutputType::toml => SerializeType::toml,
            _ => panic!("should never happen"),
        }
    }
}

pub struct SerdeRenderer<O>(Arc<O>);

impl<O> SerdeRenderer<O>
where
    O: Terminal,
{
    pub fn new(out: Arc<O>) -> Self {
        Self(out)
    }

    pub fn render<S>(&self, value: &S, output_type: SerializeType) -> Result<(), OutputError>
    where
        S: Serialize,
    {
        match output_type {
            SerializeType::yaml => self.to_yaml(value),
            SerializeType::json => self.to_json(value),
            SerializeType::toml => self.to_toml(value),
        }
    }

    /// convert result to yaml format and print to terminal
    fn to_yaml<S>(&self, value: &S) -> Result<(), OutputError>
    where
        S: Serialize,
    {
        let serialized = serde_yaml::to_string(value)?;

        self.0.println(&serialized);

        Ok(())
    }

    /// convert to yaml format and print to terminal
    fn to_json<S>(&self, value: &S) -> Result<(), OutputError>
    where
        S: Serialize,
    {
        let serialized = serde_json::to_string_pretty(value)?;

        self.0.println(&serialized);

        Ok(())
    }

    /// convert to toml format and print to terminal
    fn to_toml<S>(&self, value: &S) -> Result<(), OutputError>
    where
        S: Serialize,
    {
        let serialized = toml::to_string(value)?;

        self.0.println(&serialized);

        Ok(())
    }
}
