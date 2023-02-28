use std::{fmt::Debug, path::PathBuf, str::FromStr};

use anyhow::{Result, Error, anyhow};

//use cargo_builder::{package::PackageInfo, cargo::Cargo};
//use fluvio_connector_deployer::{Deployment, DeploymentType};

//use crate::{cmd::PackageCmd, deploy::from_cargo_package};


use clap::{Parser, ValueEnum};
use cargo_generate::{GenerateArgs, TemplatePath, generate};
use include_dir::{Dir, include_dir};
use tempfile::TempDir;
use enum_display::EnumDisplay;
use tracing::debug;
use lib_cargo_crate::{Info, InfoOpts};
//use toml::Value;

use fluvio_hub_util::HubAccess;
//use crate::load::DEFAULT_META_LOCATION as SMARTMODULE_META_FILENAME;

static CONNECTOR_MODULE_TEMPLATE: Dir<'static> =
    include_dir!("$CARGO_MANIFEST_DIR/../../connector/cargo_template");
const FLUVIO_CONNECTORS_COMMON_CRATE_NAME: &str = "fluvio-connector-common";
const FLUVIO_SMARTMODULE_REPO: &str = "https://github.com/infinyon/fluvio.git";

/// Generate new SmartModule project
#[derive(Debug, Parser)]
pub struct GenerateCmd {
    /// SmartModule Project Name
    name: Option<String>,

    /// SmartModule Project Group Name.
    /// Default to Hub ID, if set. Overrides Hub ID if provided.
    #[clap(long, env = "CDK_PROJECT_GROUP", value_name = "GROUP")]
    project_group: Option<String>,

    /// Local path to generate the SmartModule project.
    /// Default to directory with project name, created in current directory
    #[clap(long, env = "CDK_DESTINATION", value_name = "PATH")]
    destination: Option<PathBuf>,

    /// Disable interactive prompt. Take all values from CLI flags. Fail if a value is missing.
    #[clap(long, action, hide_short_help = true)]
    silent: bool,

    /// Visibility of SmartModule project to generate.
    /// Skip prompt if value given.
    #[clap(long, value_enum, value_name = "PUBLIC", env = "CDK_CONN_PUBLIC")]
    conn_public: Option<bool>,

    /// Set the remote URL for the hub
    #[clap(long, env = "SMDK_HUB_REMOTE", hide_short_help = true)]
    hub_remote: Option<String>,
}

impl GenerateCmd {
    pub(crate) fn process(self) -> Result<()> {
        // If a name isn't specified, you'll get prompted in wizard
        if let Some(ref name) = self.name {
            println!("Generating new Connector project: {name}");
        }

        // Figure out if there's a Hub ID set
        let mut hub_config = HubAccess::default_load(&self.hub_remote)?;

        let group = if let Some(user_group) = self.project_group {
            if user_group.is_empty() {
                debug!("User requesting to be prompted for project group");
                None
            } else {
                debug!("Using user provided project group: {}", &user_group);
                Some(user_group)
            }
        } else if hub_config.hubid.is_empty() {
            debug!("No project group value set");
            None
        } else {
            debug!("Found project group: {}", hub_config.hubid.clone());
            Some(hub_config.hubid.clone())
        };

        /*
        let mut maybe_user_input = SmdkTemplateUserValues::new();
        maybe_user_input
            .with_project_group(group.clone())
            .with_smart_module_type(self.sm_type)
            .with_smart_module_params(sm_params)
            .with_smart_module_cargo_dependency(Some(sm_dep_source))
            .with_smart_module_public(self.sm_public);
        */

        // cargo generate template source
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().to_str().map(|s| s.to_string());
        CONNECTOR_MODULE_TEMPLATE
            .extract(&temp_dir)
            .map_err(Error::from)?;
        let template_path = TemplatePath {
            path,
            ..Default::default()
        };

        let args = GenerateArgs {
            name: self.name,
            template_path,
            verbose: !self.silent,
            silent: self.silent,
            //define: maybe_user_input.to_cargo_generate(),
            destination: self.destination,
            ..Default::default()
        };

        let gen_dir = generate(args).map_err(Error::from)?;

        // If group was empty, read it from the generated file
        // and write it to disk
        if group.is_none() {
            todo!();
            /*
            let sm_toml_path = gen_dir.join(SMARTMODULE_META_FILENAME);

            debug!("Extracting group from {}", sm_toml_path.display());

            let sm_str = std::fs::read_to_string(sm_toml_path)?;

            debug!("{:?}", &sm_str);

            let sm_toml: Value = toml::from_str(&sm_str)?;

            if let Value::Table(package) = &sm_toml["package"] {
                if let Some(Value::String(groupname)) = package.get("group") {
                    todo!();
                    //set_hubid(groupname, &mut hub_config)?;
                }
            }
            */
        }

        Ok(())
    }
}
#[derive(Debug, Clone, PartialEq)]
enum CdkTemplateValue {
    //UseParams(bool),
    CdkCargoDependency(CargoConnDependSource),
    CdkType(CdkType),
    ProjectGroup(String),
    ConnectorPublic(bool),
}

impl std::fmt::Display for CdkTemplateValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CdkTemplateValue::CdkCargoDependency(dependency) => {
                write!(f, "fluvio-connector-cargo-dependency={dependency}")
            }
            CdkTemplateValue::CdkType(conn_type) => {
                write!(f, "connector-type={conn_type}")
            }
            /*
            CdkTemplateValue::UseParams(sm_params) => {
                write!(f, "smartmodule-params={sm_params}")
            }
            */
            CdkTemplateValue::ProjectGroup(group) => {
                write!(f, "project-group={group}")
            }
            CdkTemplateValue::ConnectorPublic(public) => {
                write!(f, "connector-public={public}")
            }
        }
    }
}
#[derive(ValueEnum, Clone, Debug, Parser, PartialEq, Eq, EnumDisplay)]
#[clap(rename_all = "kebab-case")]
#[enum_display(case = "Kebab")]
enum CdkType {
    Source,
    Sink,
}
#[derive(Debug, Clone, PartialEq)]
enum CargoConnDependSource {
    CratesIo(String),
    Git(String),
    GitBranch { url: String, branch: String },
    GitTag { url: String, tag: String },
    GitRev { url: String, rev: String },
    Path(PathBuf),
}

impl std::fmt::Display for CargoConnDependSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CargoConnDependSource::CratesIo(version) => {
                write!(f, "\"{version}\"")
            }
            CargoConnDependSource::Git(url) => write!(f, "{{ git = \"{url}\" }}"),
            CargoConnDependSource::GitBranch { url, branch } => {
                write!(f, "{{ git = \"{url}\", branch = \"{branch}\" }}")
            }
            CargoConnDependSource::GitTag { url, tag } => {
                write!(f, "{{ git = \"{url}\", tag = \"{tag}\" }}")
            }
            CargoConnDependSource::GitRev { url, rev } => {
                write!(f, "{{ git = \"{url}\", rev = \"{rev}\" }}")
            }
            CargoConnDependSource::Path(path) => {
                write!(f, "{{ path = \"{}\" }}", path.as_path().display())
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::fs::read_dir;

    use super::SmdkTemplate;
    use super::SmdkTemplateUserValues;
    use super::SmartModuleType;
    use super::CdkTemplateValue;
    use super::CargoConnDependSource;
    use super::FLUVIO_SMARTMODULE_REPO;

    #[test]
    fn test_default_template() {
        let template = SmdkTemplate::default().unwrap();

        assert!(
            template._temp_dir.is_some(),
            "The temporary directory reference is not provided"
        );

        let temp_dir = template._temp_dir.unwrap();
        let temp_dir = read_dir(temp_dir.path());
        assert!(temp_dir.is_ok(), "The temporary directory doesn't exists");

        let mut temp_dir = temp_dir.unwrap();
        let smart_toml =
            temp_dir.find(|entry| entry.as_ref().unwrap().file_name().eq("SmartModule.toml"));

        assert!(
            smart_toml.is_some(),
            "Smart.toml from template is not included in temporary dir"
        );
        assert!(smart_toml.unwrap().is_ok());
    }

    #[test]
    fn test_cargo_dependency_values() {
        let test_semver = "4.5.6".to_string();
        let test_template_values = vec![
            CargoConnDependSource::CratesIo(test_semver),
            CargoConnDependSource::Git(FLUVIO_SMARTMODULE_REPO.to_string()),
            CargoConnDependSource::GitBranch {
                url: FLUVIO_SMARTMODULE_REPO.to_string(),
                branch: "my-branch".to_string(),
            },
            CargoConnDependSource::GitTag {
                url: FLUVIO_SMARTMODULE_REPO.to_string(),
                tag: "my-tag".to_string(),
            },
            CargoConnDependSource::GitRev {
                url: FLUVIO_SMARTMODULE_REPO.to_string(),
                rev: "abcdef01189998119991197253".to_string(),
            },
        ];

        for value in test_template_values {
            match &value {
                CargoConnDependSource::CratesIo(version) => {
                    assert_eq!(value.to_string(), format!("\"{version}\""))
                }
                CargoConnDependSource::Git(url) => {
                    assert_eq!(value.to_string(), format!("{{ git = \"{url}\" }}"))
                }
                CargoConnDependSource::GitBranch { url, branch } => {
                    assert_eq!(
                        value.to_string(),
                        format!("{{ git = \"{url}\", branch = \"{branch}\" }}")
                    )
                }
                CargoConnDependSource::GitTag { url, tag } => {
                    assert_eq!(
                        value.to_string(),
                        format!("{{ git = \"{url}\", tag = \"{tag}\" }}")
                    )
                }
                CargoConnDependSource::GitRev { url, rev } => {
                    assert_eq!(
                        value.to_string(),
                        format!("{{ git = \"{url}\", rev = \"{rev}\" }}")
                    )
                }
                CargoConnDependSource::Path(path) => {
                    assert_eq!(
                        value.to_string(),
                        format!("{{ path = \"{}\" }}", path.as_path().display())
                    )
                }
            }
        }
    }

    #[test]
    fn test_generate_user_values() {
        let test_template_values = vec![
            CdkTemplateValue::UseParams(true),
            CdkTemplateValue::SmCargoDependency(CargoConnDependSource::CratesIo(
                "0.1.0".to_string(),
            )),
            CdkTemplateValue::SmType(SmartModuleType::FilterMap),
            CdkTemplateValue::ProjectGroup("ExampleGroupName".to_string()),
        ];

        for value in test_template_values {
            match value {
                CdkTemplateValue::UseParams(_) => {
                    assert_eq!(
                        &CdkTemplateValue::UseParams(true).to_string(),
                        "smartmodule-params=true"
                    );
                }

                CdkTemplateValue::SmCargoDependency(_) => {
                    assert_eq!(
                        &CdkTemplateValue::SmCargoDependency(CargoConnDependSource::CratesIo(
                            "0.1.0".to_string()
                        ))
                        .to_string(),
                        "fluvio-smartmodule-cargo-dependency=\"0.1.0\""
                    );
                }

                CdkTemplateValue::SmType(_) => {
                    assert_eq!(
                        &CdkTemplateValue::SmType(SmartModuleType::FilterMap).to_string(),
                        "smartmodule-type=filter-map"
                    );
                }

                CdkTemplateValue::ProjectGroup(_) => {
                    assert_eq!(
                        &CdkTemplateValue::ProjectGroup("ExampleGroupName".to_string())
                            .to_string(),
                        "project-group=ExampleGroupName"
                    );
                }
                CdkTemplateValue::ConnectorPublic(_) => {
                    assert_eq!(
                        &CdkTemplateValue::ConnectorPublic(true).to_string(),
                        "smartmodule-public=true"
                    );
                    assert_eq!(
                        &CdkTemplateValue::ConnectorPublic(false).to_string(),
                        "smartmodule-public=false"
                    );
                }
            }
        }
    }

    #[test]
    fn test_template_builder() {
        let mut values = SmdkTemplateUserValues::new();
        let test_version_number = "test-version-value".to_string();
        values
            .with_project_group(Some("ExampleGroupName".to_string()))
            .with_smart_module_type(Some(SmartModuleType::Aggregate))
            .with_smart_module_params(Some(true))
            .with_smart_module_cargo_dependency(Some(CargoConnDependSource::CratesIo(
                test_version_number.clone(),
            )))
            .with_smart_module_public(Some(false));

        let values_vec = values.to_vec();

        for v in values_vec {
            match v {
                CdkTemplateValue::UseParams(_) => {
                    assert_eq!(v, CdkTemplateValue::UseParams(true));
                }

                CdkTemplateValue::SmCargoDependency(_) => {
                    assert_eq!(
                        v,
                        CdkTemplateValue::SmCargoDependency(CargoConnDependSource::CratesIo(
                            test_version_number.clone()
                        ))
                    );
                }

                CdkTemplateValue::SmType(_) => {
                    assert_eq!(v, CdkTemplateValue::SmType(SmartModuleType::Aggregate));
                }

                CdkTemplateValue::ProjectGroup(_) => {
                    assert_eq!(
                        v,
                        CdkTemplateValue::ProjectGroup("ExampleGroupName".to_string())
                    );
                }
                CdkTemplateValue::ConnectorPublic(_) => {
                    assert_eq!(v, CdkTemplateValue::ConnectorPublic(false));
                }
            }
        }
    }
}
