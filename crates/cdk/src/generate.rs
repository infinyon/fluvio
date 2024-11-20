use std::fmt::Debug;
use std::fmt::Display;
use std::path::PathBuf;

use anyhow::{Result, Error};

use clap::{Parser, ValueEnum};
use cargo_generate::{GenerateArgs, TemplatePath, generate};
use include_dir::{Dir, include_dir};
use tempfile::TempDir;
use enum_display::EnumDisplay;

// Note: Cargo.toml.liquid files are changed by cargo-generate to Cargo.toml
// this avoids the problem of cargo trying to parse Cargo.toml template files
// and generating a lot of parsing errors

static CONNECTOR_TEMPLATE: Dir<'static> =
    include_dir!("$CARGO_MANIFEST_DIR/../../connector/cargo_template");

/// Generate new Connector project
#[derive(Debug, Parser)]
pub struct GenerateCmd {
    /// Connector Name
    name: Option<String>,

    #[arg(long, value_name = "GROUP")]
    /// Connector developer group
    group: Option<String>,

    /// Connector description used as part of the project metadata
    #[arg(long, value_name = "DESCRIPTION")]
    conn_description: Option<String>,

    /// Local path to generate the Connector project.
    /// Default to directory with project name, created in current directory
    #[arg(long, env = "CDK_DESTINATION", value_name = "PATH")]
    destination: Option<PathBuf>,

    /// Disable interactive prompt. Take all values from CLI flags. Fail if a value is missing.
    #[arg(long, hide_short_help = true)]
    silent: bool,

    /// Type of Connector project to generate.
    /// Skip prompt if value given.
    #[arg(long, value_enum, value_name = "TYPE", env = "CDK_CONN_TYPE")]
    conn_type: Option<ConnectorType>,

    /// Visibility of Connector project to generate.
    /// Skip prompt if value given.
    #[arg(long, value_enum, value_name = "PUBLIC", env = "CDK_CONN_PUBLIC")]
    conn_public: Option<bool>,
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

        let mut maybe_user_input = CdkTemplateUserValues::new();

        maybe_user_input
            .with_name(self.name.clone())
            .with_group(self.group)
            .with_description(self.conn_description)
            .with_conn_type(self.conn_type)
            .with_conn_public(self.conn_public);

        let args = GenerateArgs {
            name: self.name,
            template_path,
            verbose: !self.silent,
            silent: self.silent,
            destination: self.destination,
            define: maybe_user_input.to_cargo_generate(),
            ..Default::default()
        };

        let _gen_dir = generate(args).map_err(Error::from)?;

        Ok(())
    }
}

#[derive(ValueEnum, Clone, Debug, Parser, PartialEq, Eq, EnumDisplay)]
#[clap(rename_all = "kebab-case")]
#[enum_display(case = "Kebab")]
enum ConnectorType {
    Sink,
    Source,
}

#[derive(Clone, Debug)]
enum CdkTemplateValue {
    Name(String),
    Group(String),
    Description(String),
    ConnFluvioDependencyHash(String),
    ConnType(ConnectorType),
    ConnPublic(bool),
}

impl Display for CdkTemplateValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CdkTemplateValue::Name(name) => write!(f, "project-name={}", name),
            CdkTemplateValue::Group(group) => write!(f, "project-group={}", group),
            CdkTemplateValue::Description(description) => {
                write!(f, "project-description={}", description)
            }
            CdkTemplateValue::ConnFluvioDependencyHash(hash) => {
                write!(f, "fluvio-cargo-dependency-hash={}", hash)
            }
            CdkTemplateValue::ConnType(conn_type) => write!(f, "connector-type={conn_type}"),
            CdkTemplateValue::ConnPublic(conn_public) => {
                write!(f, "connector-public={conn_public}")
            }
        }
    }
}

#[derive(Debug, Default, Clone)]
struct CdkTemplateUserValues {
    values: Vec<CdkTemplateValue>,
}

impl CdkTemplateUserValues {
    fn new() -> Self {
        // By default the fluvio dependency hash is the current git hash
        // and its always passed as option to cargo generate
        let values = vec![CdkTemplateValue::ConnFluvioDependencyHash(
            env!("GIT_HASH").to_string(),
        )];

        Self { values }
    }

    fn to_vec(&self) -> Vec<CdkTemplateValue> {
        self.values.clone()
    }

    fn to_cargo_generate(&self) -> Vec<String> {
        self.to_vec().iter().map(|v| v.to_string()).collect()
    }

    fn with_name(&mut self, value: Option<String>) -> &mut Self {
        if let Some(v) = value {
            tracing::debug!("CDK Argument - project-name={}", v);
            self.values.push(CdkTemplateValue::Name(v));
        }

        self
    }

    fn with_group(&mut self, value: Option<String>) -> &mut Self {
        if let Some(v) = value {
            tracing::debug!("CDK Argument - project-group={}", v);
            self.values.push(CdkTemplateValue::Group(v));
        }

        self
    }

    fn with_description(&mut self, value: Option<String>) -> &mut Self {
        if let Some(v) = value {
            tracing::debug!("CDK Argument - project-description={}", v);
            self.values.push(CdkTemplateValue::Description(v));
        }

        self
    }

    fn with_conn_type(&mut self, value: Option<ConnectorType>) -> &mut Self {
        if let Some(v) = value {
            tracing::debug!("CDK Argument - connector-type={}", v);
            self.values.push(CdkTemplateValue::ConnType(v));
        }

        self
    }

    fn with_conn_public(&mut self, value: Option<bool>) -> &mut Self {
        if let Some(v) = value {
            tracing::debug!("CDK Argument - connector-public={}", v);
            self.values.push(CdkTemplateValue::ConnPublic(v));
        }

        self
    }
}
