use std::sync::Arc;

use serde::Serialize;
use structopt::clap::arg_enum;

use crate::Terminal;
use crate::CliError;

use super::OutputType;

arg_enum! {
    #[derive(Debug, Clone, PartialEq)]
    #[allow(non_camel_case_types)]
    pub enum SerializeType {
        yaml,
        json,
    }
}

impl From<OutputType> for SerializeType {
    fn from(output: OutputType) -> Self {
        match output {
            OutputType::yaml => SerializeType::yaml,
            OutputType::json => SerializeType::json,
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

    pub fn render<S>(&self, value: &S, output_type: SerializeType) -> Result<(), CliError>
    where
        S: Serialize,
    {
        match output_type {
            SerializeType::yaml => self.to_yaml(value),
            SerializeType::json => self.to_json(value),
        }
    }

    /// convert result to yaml format and print to terminal
    fn to_yaml<S>(&self, value: &S) -> Result<(), CliError>
    where
        S: Serialize,
    {
        let serialized = serde_yaml::to_string(value).unwrap();

        self.0.println(&serialized);

        Ok(())
    }

    /// convert to yaml format and print to terminal
    fn to_json<S>(&self, value: &S) -> Result<(), CliError>
    where
        S: Serialize,
    {
        let serialized = serde_json::to_string_pretty(value).unwrap();

        self.0.println(&serialized);

        Ok(())
    }
}
