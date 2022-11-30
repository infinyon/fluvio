use std::{path::PathBuf, str::FromStr};

use anyhow::{Error, Result, anyhow};
use clap::{Parser, ValueEnum};
use cargo_generate::{GenerateArgs, TemplatePath, generate};
use include_dir::{Dir, include_dir};
use tempfile::TempDir;
use enum_display::EnumDisplay;
use tracing::debug;
use lib_cargo_crate::{Info, InfoOpts};
use toml::Value;

use fluvio_hub_util::HubAccess;
use crate::hub::set_hubid;
use crate::load::DEFAULT_META_LOCATION as SMARTMODULE_META_FILENAME;

static SMART_MODULE_TEMPLATE: Dir<'static> =
    include_dir!("$CARGO_MANIFEST_DIR/../../smartmodule/cargo_template");
const FLUVIO_SMARTMODULE_CRATE_NAME: &str = "fluvio-smartmodule";
const FLUVIO_SMARTMODULE_REPO: &str = "https://github.com/infinyon/fluvio.git";

/// Generate new SmartModule project
#[derive(Debug, Parser)]
pub struct GenerateCmd {
    /// SmartModule Project Name
    name: Option<String>,

    /// SmartModule Project Group Name.
    /// Default to Hub ID, if set. Overrides Hub ID if provided.
    #[clap(long, env = "SMDK_PROJECT_GROUP", value_name = "GROUP")]
    project_group: Option<String>,

    /// Local path to generate the SmartModule project.
    /// Default to directory with project name, created in current directory
    #[clap(long, env = "SMDK_DESTINATION", value_name = "PATH")]
    destination: Option<PathBuf>,

    /// Disable interactive prompt. Take all values from CLI flags. Fail if a value is missing.
    #[clap(long, action, hide_short_help = true)]
    silent: bool,

    /// URL to git repo containing templates for generating SmartModule projects.
    /// Using this option is discouraged. The default value is recommended.
    #[clap(
        long,
        hide_short_help = true,
        group("TemplateSourceGit"),
        conflicts_with = "TemplateSourcePath",
        value_name = "GIT_URL",
        env = "SMDK_TEMPLATE_REPO"
    )]
    template_repo: Option<String>,

    /// An optional git branch to use with `--template-repo`
    #[clap(
        long,
        hide_short_help = true,
        group("TemplateGit"),
        requires = "TemplateSourceGit",
        value_name = "BRANCH",
        env = "SMDK_TEMPLATE_REPO_BRANCH"
    )]
    template_repo_branch: Option<String>,

    /// An optional git tag to use with `--template-repo`
    #[clap(
        long,
        hide_short_help = true,
        group("TemplateGit"),
        requires = "TemplateSourceGit",
        value_name = "TAG",
        env = "SMDK_TEMPLATE_REPO_TAG"
    )]
    template_repo_tag: Option<String>,

    /// Local filepath containing templates for generating SmartModule projects.
    /// Using this option is discouraged. The default value is recommended.
    #[clap(
        long,
        hide_short_help = true,
        group("TemplateSourcePath"),
        conflicts_with = "TemplateSourceGit",
        value_name = "PATH",
        env = "SMDK_TEMPLATE_PATH"
    )]
    template_path: Option<String>,

    /// URL of git repo to include in generated Cargo.toml. Repo used for `fluvio-smartmodule` dependency.
    /// Using this option is discouraged. The default value is recommended.
    #[clap(
        long,
        hide_short_help = true,
        group("SmCrateSourceGit"),
        conflicts_with_all = &["SmCrateSourcePath", "SmCrateSourceCratesIo"],
        value_name = "GIT_URL",
        env = "SMDK_SM_CRATE_REPO",

    )]
    sm_crate_repo: Option<String>,

    /// An optional git branch to use with `--sm-crate-repo`
    #[clap(
        long,
        hide_short_help = true,
        group("SmGit"),
        requires = "SmCrateSourceGit",
        value_name = "BRANCH",
        env = "SMDK_SM_REPO_BRANCH"
    )]
    sm_repo_branch: Option<String>,

    /// An optional git tag to use with `--sm-crate-repo`
    #[clap(
        long,
        hide_short_help = true,
        group("SmGit"),
        requires = "SmCrateSourceGit",
        value_name = "TAG",
        env = "SMDK_SM_REPO_TAG"
    )]
    sm_repo_tag: Option<String>,

    /// An optional git rev to use with `--sm-crate-repo`
    #[clap(
        long,
        hide_short_help = true,
        group("SmGit"),
        requires = "SmCrateSourceGit",
        value_name = "GIT_SHA",
        env = "SMDK_SM_REPO_REV"
    )]
    sm_repo_rev: Option<String>,

    /// Local filepath to include in generated Cargo.toml. Path used for fluvio-smartmodule dependency.
    /// Using this option is discouraged. The default value is recommended.
    #[clap(
        long,
        hide_short_help = true,
        group("SmCrateSourcePath"),
        conflicts_with_all = &["SmCrateSourceGit", "SmCrateSourceCratesIo"],
        value_name = "PATH",
        env = "SMDK_SM_CRATE_PATH"
    )]
    sm_crate_path: Option<String>,

    /// Public version of `fluvio-smartmodule` from crates.io. Defaults to latest.
    /// Using this option is discouraged. The default value is recommended.
    #[clap(
        long,
        hide_short_help = true,
        group("SmCrateSourceCratesIo"),
        conflicts_with_all = &["SmCrateSourceGit", "SmCrateSourcePath"],
        value_name = "X.Y.Z",
        env = "SMDK_SM_CRATE_VERSION"
    )]
    sm_crate_version: Option<String>,

    /// Type of SmartModule project to generate.
    /// Skip prompt if value given.
    #[clap(long, value_enum, value_name = "TYPE", env = "SMDK_SM_TYPE")]
    sm_type: Option<SmartModuleType>,

    /// Visibility of SmartModule project to generate.
    /// Skip prompt if value given.
    #[clap(long, value_enum, value_name = "PUBLIC", env = "SMDK_SM_PUBLIC")]
    sm_public: Option<bool>,

    /// Include SmartModule input parameters in generated SmartModule project.
    /// Skip prompt if value given.
    #[clap(long, group("SmartModuleParams"), action, env = "SMDK_WITH_PARAMS")]
    with_params: bool,

    /// No SmartModule input parameters in generated SmartModule project.
    /// Skip prompt if value given.
    #[clap(long, group("SmartModuleParams"), action, env = "SMDK_NO_PARAMS")]
    no_params: bool,

    /// Set the remote URL for the hub
    #[clap(long, env = "SMDK_HUB_REMOTE", hide_short_help = true)]
    hub_remote: Option<String>,

    /// Using this option will always choose the Fluvio repo as source for templates and dependencies
    #[clap(long, action, env = "SMDK_DEVELOP", hide_short_help = true, conflicts_with_all =
        &["TemplateSourceGit", "TemplateSourcePath",
        "SmCrateSourceGit", "SmCrateSourceCratesIo", "SmCrateSourcePath"],)]
    develop: bool,
}

impl GenerateCmd {
    pub(crate) fn process(self) -> Result<()> {
        // If a name isn't specified, you'll get prompted in wizard
        if let Some(ref name) = self.name {
            println!("Generating new SmartModule project: {}", name);
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

        let sm_params = match (self.with_params, self.no_params) {
            (true, false) => Some(true),
            (false, true) => Some(false),
            _ => None,
        };

        // fluvio-smartmodule source
        // Check: user version, user git, user path, develop, then default to latest crates.io version
        let sm_dep_source = if let Some(user_version) = self.sm_crate_version {
            CargoSmDependSource::CratesIo(user_version)
        } else if let Some(user_repo) = self.sm_crate_repo {
            if let Some(branch) = self.sm_repo_branch {
                CargoSmDependSource::GitBranch {
                    url: user_repo,
                    branch,
                }
            } else if let Some(tag) = self.sm_repo_tag {
                CargoSmDependSource::GitTag {
                    url: user_repo,
                    tag,
                }
            } else if let Some(rev) = self.sm_repo_rev {
                CargoSmDependSource::GitRev {
                    url: user_repo,
                    rev,
                }
            } else {
                CargoSmDependSource::Git(FLUVIO_SMARTMODULE_REPO.to_string())
            }
        } else if let Some(path) = self.sm_crate_path {
            CargoSmDependSource::Path(PathBuf::from_str(&path)?)
        } else if self.develop {
            CargoSmDependSource::Git(FLUVIO_SMARTMODULE_REPO.to_string())
        } else {
            let latest_sm_crate_info =
                Info::new().fetch(vec![FLUVIO_SMARTMODULE_CRATE_NAME], &InfoOpts::default())?;
            let version = &latest_sm_crate_info[0].krate.crate_data.max_version;

            CargoSmDependSource::CratesIo(version.to_string())
        };

        let mut maybe_user_input = SmdkTemplateUserValues::new();
        maybe_user_input
            .with_project_group(group.clone())
            .with_smart_module_type(self.sm_type)
            .with_smart_module_params(sm_params)
            .with_smart_module_cargo_dependency(Some(sm_dep_source))
            .with_smart_module_public(self.sm_public);

        // cargo generate template source
        // Check user git, user path, develop, then default to the built-in
        let SmdkTemplate {
            template_path,
            _temp_dir,
            ..
        } = if let Some(git_url) = self.template_repo {
            if let Some(branch) = self.template_repo_branch {
                SmdkTemplate::source(SmdkTemplateSource::GitBranch {
                    url: git_url,
                    branch,
                })?
            } else if let Some(tag) = self.template_repo_tag {
                SmdkTemplate::source(SmdkTemplateSource::GitTag { url: git_url, tag })?
            } else {
                SmdkTemplate::source(SmdkTemplateSource::Git(git_url))?
            }
        } else if let Some(path) = self.template_path {
            SmdkTemplate::source(SmdkTemplateSource::LocalPath(path.into()))?
        } else if self.develop {
            SmdkTemplate::source(SmdkTemplateSource::Git(FLUVIO_SMARTMODULE_REPO.to_string()))?
        } else {
            SmdkTemplate::default()?
        };

        let args = GenerateArgs {
            template_path,
            name: self.name,
            verbose: !self.silent,
            silent: self.silent,
            define: maybe_user_input.to_cargo_generate(),
            destination: self.destination,
            ..Default::default()
        };

        let gen_dir = generate(args).map_err(Error::from)?;

        // If group was empty, read it from the generated file
        // and write it to disk
        if group.is_none() {
            let sm_toml_path = gen_dir.join(SMARTMODULE_META_FILENAME);

            debug!("Extracting group from {}", sm_toml_path.display());

            let sm_str = std::fs::read_to_string(sm_toml_path)?;

            debug!("{:?}", &sm_str);

            let sm_toml: Value = toml::from_str(&sm_str)?;

            if let Value::Table(package) = &sm_toml["package"] {
                if let Some(Value::String(groupname)) = package.get("group") {
                    set_hubid(groupname, &mut hub_config)?;
                }
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
enum SmdkTemplateValue {
    UseParams(bool),
    SmCargoDependency(CargoSmDependSource),
    SmType(SmartModuleType),
    ProjectGroup(String),
    SmPublic(bool),
}

impl std::fmt::Display for SmdkTemplateValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SmdkTemplateValue::SmCargoDependency(dependency) => {
                write!(f, "fluvio-smartmodule-cargo-dependency={}", dependency)
            }
            SmdkTemplateValue::SmType(sm_type) => {
                write!(f, "smartmodule-type={}", sm_type)
            }
            SmdkTemplateValue::UseParams(sm_params) => {
                write!(f, "smartmodule-params={}", sm_params)
            }
            SmdkTemplateValue::ProjectGroup(group) => {
                write!(f, "project-group={}", group)
            }
            SmdkTemplateValue::SmPublic(public) => {
                write!(f, "smartmodule-public={}", public)
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

/// Abstraction on different of template options available for generating a
/// new SmartModule project.
///
/// May hold a reference to a `TempDir` which should not be dropped before
/// accomplishing the project generation procedure.
struct SmdkTemplate {
    template_path: TemplatePath,
    _temp_dir: Option<TempDir>,
    _template_source: SmdkTemplateSource,
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
                path,
                ..Default::default()
            },
            _temp_dir: Some(temp_dir),
            _template_source: SmdkTemplateSource::Default,
        };

        Ok(template)
    }

    /// Configure where to find Smdk templates
    fn source(source: SmdkTemplateSource) -> Result<Self> {
        match source.clone() {
            SmdkTemplateSource::Git(url) => {
                debug!("Selecting templates from git @ {url}");
                Ok(Self {
                    template_path: TemplatePath {
                        git: Some(url),
                        ..Default::default()
                    },
                    _temp_dir: None,
                    _template_source: source,
                })
            }
            SmdkTemplateSource::GitTag { url, tag } => {
                debug!("Selecting templates from git @ {url} (tag {tag})");
                Ok(Self {
                    template_path: TemplatePath {
                        git: Some(url),
                        tag: Some(tag),
                        ..Default::default()
                    },
                    _temp_dir: None,
                    _template_source: source,
                })
            }
            SmdkTemplateSource::GitBranch { url, branch } => {
                debug!("Selecting templates from git @ {url} (branch {branch})");
                Ok(Self {
                    template_path: TemplatePath {
                        git: Some(url),
                        branch: Some(branch),
                        ..Default::default()
                    },
                    _temp_dir: None,
                    _template_source: source,
                })
            }
            SmdkTemplateSource::LocalPath(path) => {
                debug!("Selecting templates from local path @ {path:?}");

                let path_str = path
                    .to_str()
                    .ok_or_else(|| anyhow!("Error extracting path from input"))?
                    .to_string();
                Ok(Self {
                    template_path: TemplatePath {
                        path: Some(path_str),
                        ..Default::default()
                    },
                    _temp_dir: None,
                    _template_source: source,
                })
            }
            SmdkTemplateSource::Default => Self::default(),
        }
    }
}

#[derive(Default, Clone)]
enum SmdkTemplateSource {
    #[default]
    Default,
    Git(String),
    GitTag {
        url: String,
        tag: String,
    },
    GitBranch {
        url: String,
        branch: String,
    },
    LocalPath(PathBuf),
}

#[derive(Debug, Default, Clone)]
struct SmdkTemplateUserValues {
    values: Vec<SmdkTemplateValue>,
}

impl SmdkTemplateUserValues {
    fn new() -> Self {
        SmdkTemplateUserValues::default()
    }

    fn with_smart_module_cargo_dependency(
        &mut self,
        dependency: Option<CargoSmDependSource>,
    ) -> &mut Self {
        if let Some(d) = dependency {
            debug!("User provided fluvio-smartmodule Cargo.toml value: {d:#?}");
            self.values.push(SmdkTemplateValue::SmCargoDependency(d));
        }
        self
    }

    fn with_smart_module_type(&mut self, sm_type: Option<SmartModuleType>) -> &mut Self {
        if let Some(t) = sm_type {
            debug!("User provided SmartModule type: {t:#?}");
            self.values.push(SmdkTemplateValue::SmType(t));
        }
        self
    }

    fn with_smart_module_params(&mut self, request: Option<bool>) -> &mut Self {
        if let Some(i) = request {
            debug!("User provided SmartModule params request: {i:#?}");
            self.values.push(SmdkTemplateValue::UseParams(i));
        }
        self
    }

    fn with_project_group(&mut self, group: Option<String>) -> &mut Self {
        if let Some(i) = group {
            debug!("User default project group: {i:#?}");
            self.values.push(SmdkTemplateValue::ProjectGroup(i));
        }
        self
    }

    fn with_smart_module_public(&mut self, public: Option<bool>) -> &mut Self {
        if let Some(p) = public {
            debug!("User project public: {p:#?}");
            self.values.push(SmdkTemplateValue::SmPublic(p));
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

#[derive(Debug, Clone, PartialEq)]
enum CargoSmDependSource {
    CratesIo(String),
    Git(String),
    GitBranch { url: String, branch: String },
    GitTag { url: String, tag: String },
    GitRev { url: String, rev: String },
    Path(PathBuf),
}

impl std::fmt::Display for CargoSmDependSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CargoSmDependSource::CratesIo(version) => {
                write!(f, "\"{}\"", version)
            }
            CargoSmDependSource::Git(url) => write!(f, "{{ git = \"{}\" }}", url),
            CargoSmDependSource::GitBranch { url, branch } => {
                write!(f, "{{ git = \"{}\", branch = \"{}\" }}", url, branch)
            }
            CargoSmDependSource::GitTag { url, tag } => {
                write!(f, "{{ git = \"{}\", tag = \"{}\" }}", url, tag)
            }
            CargoSmDependSource::GitRev { url, rev } => {
                write!(f, "{{ git = \"{}\", rev = \"{}\" }}", url, rev)
            }
            CargoSmDependSource::Path(path) => {
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
    use super::SmdkTemplateValue;
    use super::CargoSmDependSource;
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
            CargoSmDependSource::CratesIo(test_semver),
            CargoSmDependSource::Git(FLUVIO_SMARTMODULE_REPO.to_string()),
            CargoSmDependSource::GitBranch {
                url: FLUVIO_SMARTMODULE_REPO.to_string(),
                branch: "my-branch".to_string(),
            },
            CargoSmDependSource::GitTag {
                url: FLUVIO_SMARTMODULE_REPO.to_string(),
                tag: "my-tag".to_string(),
            },
            CargoSmDependSource::GitRev {
                url: FLUVIO_SMARTMODULE_REPO.to_string(),
                rev: "abcdef01189998119991197253".to_string(),
            },
        ];

        for value in test_template_values {
            match &value {
                CargoSmDependSource::CratesIo(version) => {
                    assert_eq!(value.to_string(), format!("\"{}\"", version))
                }
                CargoSmDependSource::Git(url) => {
                    assert_eq!(value.to_string(), format!("{{ git = \"{}\" }}", url))
                }
                CargoSmDependSource::GitBranch { url, branch } => {
                    assert_eq!(
                        value.to_string(),
                        format!("{{ git = \"{}\", branch = \"{}\" }}", url, branch)
                    )
                }
                CargoSmDependSource::GitTag { url, tag } => {
                    assert_eq!(
                        value.to_string(),
                        format!("{{ git = \"{}\", tag = \"{}\" }}", url, tag)
                    )
                }
                CargoSmDependSource::GitRev { url, rev } => {
                    assert_eq!(
                        value.to_string(),
                        format!("{{ git = \"{}\", rev = \"{}\" }}", url, rev)
                    )
                }
                CargoSmDependSource::Path(path) => {
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
            SmdkTemplateValue::UseParams(true),
            SmdkTemplateValue::SmCargoDependency(CargoSmDependSource::CratesIo(
                "0.1.0".to_string(),
            )),
            SmdkTemplateValue::SmType(SmartModuleType::FilterMap),
            SmdkTemplateValue::ProjectGroup("ExampleGroupName".to_string()),
        ];

        for value in test_template_values {
            match value {
                SmdkTemplateValue::UseParams(_) => {
                    assert_eq!(
                        &SmdkTemplateValue::UseParams(true).to_string(),
                        "smartmodule-params=true"
                    );
                }

                SmdkTemplateValue::SmCargoDependency(_) => {
                    assert_eq!(
                        &SmdkTemplateValue::SmCargoDependency(CargoSmDependSource::CratesIo(
                            "0.1.0".to_string()
                        ))
                        .to_string(),
                        "fluvio-smartmodule-cargo-dependency=\"0.1.0\""
                    );
                }

                SmdkTemplateValue::SmType(_) => {
                    assert_eq!(
                        &SmdkTemplateValue::SmType(SmartModuleType::FilterMap).to_string(),
                        "smartmodule-type=filter-map"
                    );
                }

                SmdkTemplateValue::ProjectGroup(_) => {
                    assert_eq!(
                        &SmdkTemplateValue::ProjectGroup("ExampleGroupName".to_string())
                            .to_string(),
                        "project-group=ExampleGroupName"
                    );
                }
                SmdkTemplateValue::SmPublic(_) => {
                    assert_eq!(
                        &SmdkTemplateValue::SmPublic(true).to_string(),
                        "smartmodule-public=true"
                    );
                    assert_eq!(
                        &SmdkTemplateValue::SmPublic(false).to_string(),
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
            .with_smart_module_cargo_dependency(Some(CargoSmDependSource::CratesIo(
                test_version_number.clone(),
            )))
            .with_smart_module_public(Some(false));

        let values_vec = values.to_vec();

        for v in values_vec {
            match v {
                SmdkTemplateValue::UseParams(_) => {
                    assert_eq!(v, SmdkTemplateValue::UseParams(true));
                }

                SmdkTemplateValue::SmCargoDependency(_) => {
                    assert_eq!(
                        v,
                        SmdkTemplateValue::SmCargoDependency(CargoSmDependSource::CratesIo(
                            test_version_number.clone()
                        ))
                    );
                }

                SmdkTemplateValue::SmType(_) => {
                    assert_eq!(v, SmdkTemplateValue::SmType(SmartModuleType::Aggregate));
                }

                SmdkTemplateValue::ProjectGroup(_) => {
                    assert_eq!(
                        v,
                        SmdkTemplateValue::ProjectGroup("ExampleGroupName".to_string())
                    );
                }
                SmdkTemplateValue::SmPublic(_) => {
                    assert_eq!(v, SmdkTemplateValue::SmPublic(false));
                }
            }
        }
    }
}
