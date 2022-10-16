use anyhow::{Error, Result};
use clap::{Parser, ValueEnum, value_parser};
use cargo_generate::{GenerateArgs, TemplatePath, generate};
use include_dir::{Dir, include_dir};
use tempfile::TempDir;
use enum_display::EnumDisplay;
use tracing::debug;

static SMART_MODULE_TEMPLATE: Dir<'static> =
    include_dir!("$CARGO_MANIFEST_DIR/../../smartmodule/cargo_template");

#[derive(Debug, Clone, PartialEq)]
enum SmdkTemplateValue {
    SmartModuleInitFn(bool),
    SmartModuleParameters(bool),
    SmartModuleVersion(String),
    SmartModuleType(SmartModuleType),
}

impl std::fmt::Display for SmdkTemplateValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &*self {
            SmdkTemplateValue::SmartModuleInitFn(init) => {
                write!(f, "smart-module-init={}", init)
            }
            SmdkTemplateValue::SmartModuleVersion(version) => {
                write!(f, "smart-module-version={}", version)
            }
            SmdkTemplateValue::SmartModuleType(sm_type) => {
                write!(f, "smart-module-type={}", sm_type)
            }
            SmdkTemplateValue::SmartModuleParameters(sm_params) => {
                write!(f, "smart-module-params={}", sm_params)
            }
        }
    }
}

#[derive(ValueEnum, Clone, Debug, Parser, PartialEq, Eq, EnumDisplay)]
#[clap(rename_all = "kebab-case")]
#[enum_display(case = "Kebab")]
enum SmartModuleType {
    Filter,
    Map,
    ArrayMap,
    Aggregate,
    FilterMap,
}

// TODO: After clap update, use global_setting = AppSettings::DeriveDisplayOrder
/// Generate new SmartModule project
#[derive(Debug, Parser)]
pub struct GenerateOpt {
    /// SmartModule Project Name
    name: String,

    /// URL to git repo containing the templates for generating SmartModule projects.
    /// Using this option is discouraged. The default value is recommended.
    #[clap(long, group("TemplateSource"))]
    smdk_template_repo: Option<String>,
    // add branch
    // add tag

    // add path
    //#[clap(long, group("TemplateSource"))]
    //smdk_template_path: Option<String>,
    /// Crate version or URL to `fluvio-smartmodule` git repo generated Cargo.toml.
    /// Using this option is discouraged. The default value is recommended.
    #[clap(long, group("TemplateValue"))]
    smart_module_crate_version: Option<String>, // maybe call this smdk_smart_module_crate

    /// Type of SmartModule project to generate.
    /// Skip prompt if value given.
    #[clap(long, value_enum, group("TemplateValue"))]
    smart_module_type: Option<SmartModuleType>,

    /// Include SmartModule state initialization function in generated SmartModule project.
    /// Skip prompt if value given.
    #[clap(long, value_parser = value_parser!(bool), group("TemplateValue"))]
    add_init_fn: Option<bool>,

    /// Include SmartModule input parameters in generated SmartModule project.
    /// Skip prompt if value given.
    #[clap(long, value_parser = value_parser!(bool), group("TemplateValue"))]
    smart_module_params: Option<bool>,
    // Add destination, for selecting another directory
    // Add overwrite
}

/// Abstraction on different of template options available for generating a
/// new SmartModule project.
///
/// May hold a reference to a `TempDir` which should not be dropped before
/// accomplishing the project generation procedure.
struct SmdkTemplate {
    template_path: TemplatePath,
    _temp_dir: Option<TempDir>,
    _template_source: SmdkTemplateType,
}

enum SmdkTemplateType {
    Default,
    Git,
    //Path,
}

impl SmdkTemplate {
    /// Extracts directory contents inlined during build into a temporary directory and
    /// builds a `TemplatePath` instance with the `path` pointing to the temp
    /// directory created.
    ///
    /// Is important to hold the reference to the `_temp_dir` until generation
    /// process is completed, otherwise the temp directory will be deleted
    /// before reaching the generation process.
    fn default() -> Result<Self> {
        debug!("Selecting default templates");
        let temp_dir = TempDir::new()?;
        let path = temp_dir.path().to_str().map(|s| s.to_string());
        SMART_MODULE_TEMPLATE
            .extract(&temp_dir)
            .map_err(Error::from)?;
        let template = Self {
            template_path: TemplatePath {
                git: None,
                auto_path: None,
                subfolder: None,
                test: false,
                branch: None,
                tag: None,
                path: path,
                favorite: None,
            },
            _temp_dir: Some(temp_dir),
            _template_source: SmdkTemplateType::Default,
        };

        Ok(template)
    }

    fn git(repo_uri: String) -> Result<Self> {
        debug!("Selecting git templates from {repo_uri}");
        Ok(Self {
            template_path: TemplatePath {
                git: Some(repo_uri),
                auto_path: None,
                subfolder: None,
                test: false,
                branch: None,
                tag: None,
                path: None,
                favorite: None,
            },
            _temp_dir: None,
            _template_source: SmdkTemplateType::Git,
        })
    }
}

#[derive(Debug, Default, Clone)]
struct SmdkTemplateUserValues {
    values: Vec<SmdkTemplateValue>,
}

impl SmdkTemplateUserValues {
    fn new() -> Self {
        SmdkTemplateUserValues::default()
    }

    fn with_smart_module_crate_version(&mut self, version: Option<String>) -> &mut Self {
        if let Some(v) = version {
            debug!("User provided version: {v:#?}");
            self.values.push(SmdkTemplateValue::SmartModuleVersion(v));
        }
        self
    }

    fn with_smart_module_type(&mut self, sm_type: Option<SmartModuleType>) -> &mut Self {
        if let Some(t) = sm_type {
            debug!("User provided SmartModule type: {t:#?}");
            self.values.push(SmdkTemplateValue::SmartModuleType(t));
        }
        self
    }

    fn with_init_fn(&mut self, request: Option<bool>) -> &mut Self {
        if let Some(i) = request {
            debug!("User provided init fn request: {i:#?}");
            self.values.push(SmdkTemplateValue::SmartModuleInitFn(i));
        }
        self
    }

    fn with_smart_module_params(&mut self, request: Option<bool>) -> &mut Self {
        if let Some(i) = request {
            debug!("User provided SmartModule params request: {i:#?}");
            self.values
                .push(SmdkTemplateValue::SmartModuleParameters(i));
        }
        self
    }

    fn to_vec(&self) -> Vec<SmdkTemplateValue> {
        self.values.clone()
    }

    fn to_cargo_generate(&self) -> Vec<String> {
        self.to_vec().iter().map(|v| v.to_string()).collect()
    }
}

impl GenerateOpt {
    pub(crate) fn process(self) -> Result<()> {
        println!("Generating new SmartModule project: {}", self.name);

        let mut maybe_user_input = SmdkTemplateUserValues::new();
        maybe_user_input
            .with_smart_module_type(self.smart_module_type)
            .with_init_fn(self.add_init_fn)
            .with_smart_module_params(self.smart_module_params);

        let SmdkTemplate {
            template_path,
            _temp_dir,
            _template_source,
        } = if let Some(git_uri) = self.smdk_template_repo {
            maybe_user_input.with_smart_module_crate_version(self.smart_module_crate_version);
            SmdkTemplate::git(git_uri)?
        } else {
            // FIXME: This should not default to git repo
            let sm_version = "git = \\\"https://github.com/infinyon/fluvio.git\\\"".to_string();

            maybe_user_input.with_smart_module_crate_version(Some(sm_version));
            SmdkTemplate::default()?
        };

        let args = GenerateArgs {
            template_path,
            name: Some(self.name.clone()),
            list_favorites: false,
            force: false,
            verbose: true,
            template_values_file: None,
            silent: false,
            config: None,
            vcs: None,
            lib: false,
            bin: false,
            ssh_identity: None,
            define: maybe_user_input.to_cargo_generate(),
            init: false,
            destination: None,
            force_git_init: false,
            allow_commands: false,
            overwrite: false,
            other_args: None,
        };

        generate(args).map_err(Error::from)?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::fs::read_dir;

    use super::SmdkTemplate;
    use super::SmdkTemplateUserValues;
    use super::SmartModuleType;
    use super::SmdkTemplateValue;

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
            temp_dir.find(|entry| entry.as_ref().unwrap().file_name().eq("Smart.toml"));

        assert!(
            smart_toml.is_some(),
            "Smart.toml from template is not included in temporary dir"
        );
        assert!(smart_toml.unwrap().is_ok());
    }

    #[test]
    fn test_generate_user_values() {
        let test_template_values = vec![
            SmdkTemplateValue::SmartModuleInitFn(true),
            SmdkTemplateValue::SmartModuleParameters(true),
            SmdkTemplateValue::SmartModuleVersion("version = \"0.1.0\"".to_string()),
        ];

        for value in test_template_values {
            match value {
                SmdkTemplateValue::SmartModuleInitFn(_) => {
                    assert_eq!(
                        &SmdkTemplateValue::SmartModuleInitFn(true).to_string(),
                        "smart-module-init=true"
                    );
                }
                SmdkTemplateValue::SmartModuleParameters(_) => {
                    assert_eq!(
                        &SmdkTemplateValue::SmartModuleParameters(true).to_string(),
                        "smart-module-params=true"
                    );
                }

                SmdkTemplateValue::SmartModuleVersion(_) => {
                    assert_eq!(
                        &SmdkTemplateValue::SmartModuleVersion("version = \"0.1.0\"".to_string())
                            .to_string(),
                        "smart-module-version=version = \"0.1.0\""
                    );
                }

                SmdkTemplateValue::SmartModuleType(_) => {
                    assert_eq!(
                        &SmdkTemplateValue::SmartModuleType(SmartModuleType::FilterMap).to_string(),
                        "smart-module-type=filter-map"
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
            .with_smart_module_type(Some(SmartModuleType::Aggregate))
            .with_init_fn(Some(true))
            .with_smart_module_params(Some(true))
            .with_smart_module_crate_version(Some(test_version_number.clone()));

        let values_vec = values.to_vec();

        for v in values_vec {
            match v {
                SmdkTemplateValue::SmartModuleInitFn(_) => {
                    assert_eq!(v, SmdkTemplateValue::SmartModuleInitFn(true));
                }

                SmdkTemplateValue::SmartModuleParameters(_) => {
                    assert_eq!(v, SmdkTemplateValue::SmartModuleParameters(true));
                }

                SmdkTemplateValue::SmartModuleVersion(_) => {
                    assert_eq!(
                        v,
                        SmdkTemplateValue::SmartModuleVersion(test_version_number.clone())
                    );
                }

                SmdkTemplateValue::SmartModuleType(_) => {
                    assert_eq!(
                        v,
                        SmdkTemplateValue::SmartModuleType(SmartModuleType::Aggregate)
                    );
                }
            }
        }
    }
}
