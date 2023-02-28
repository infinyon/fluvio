use std::{fmt::Debug, path::PathBuf};

use anyhow::{Result, Error};

use clap::Parser;
use cargo_generate::{GenerateArgs, TemplatePath, generate};
use include_dir::{Dir, include_dir};
use tempfile::TempDir;

static CONNECTOR_TEMPLATE: Dir<'static> =
    include_dir!("$CARGO_MANIFEST_DIR/../../connector/cargo_template");

/// Generate new SmartConnector project
#[derive(Debug, Parser)]
pub struct GenerateCmd {
    /// SmartConnector Project Name
    name: Option<String>,

    /// Local path to generate the SmartConnector project.
    /// Default to directory with project name, created in current directory
    #[clap(long, env = "CDK_DESTINATION", value_name = "PATH")]
    destination: Option<PathBuf>,

    /// Disable interactive prompt. Take all values from CLI flags. Fail if a value is missing.
    #[clap(long, action, hide_short_help = true)]
    silent: bool,
}

impl GenerateCmd {
    pub(crate) fn process(self) -> Result<()> {
        // If a name isn't specified, you'll get prompted in wizard
        if let Some(ref name) = self.name {
            println!("Generating new Connector project: {name}");
        }

        // cargo generate template source
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().to_str().map(|s| s.to_string());
        CONNECTOR_TEMPLATE.extract(&temp_dir).map_err(Error::from)?;
        let template_path = TemplatePath {
            path,
            ..Default::default()
        };

        let args = GenerateArgs {
            name: self.name,
            template_path,
            verbose: !self.silent,
            silent: self.silent,
            destination: self.destination,
            ..Default::default()
        };

        let _gen_dir = generate(args).map_err(Error::from)?;

        Ok(())
    }
}
