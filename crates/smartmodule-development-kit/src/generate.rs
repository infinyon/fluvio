use std::{path::PathBuf, path::Path, str::FromStr};
use std::fs;

use anyhow::{Error, Result, anyhow};
use clap::{Parser, ValueEnum, AppSettings};
use cargo_generate::{GenerateArgs, TemplatePath, generate};
use include_dir::{Dir, include_dir};
use tempfile::TempDir;
use enum_display::EnumDisplay;
use tracing::debug;
use lib_cargo_crate::{Info, InfoOpts};

static SMART_MODULE_TEMPLATE: Dir<'static> =
    include_dir!("$CARGO_MANIFEST_DIR/../../smartmodule/cargo_template");
const FLUVIO_SMARTMODULE_CRATE_NAME: &str = "fluvio-smartmodule";
const FLUVIO_SMARTMODULE_REPO: &str = "https://github.com/infinyon/fluvio.git";

/// Generate new SmartModule project
#[derive(Debug, Parser)]
#[clap(global_setting(AppSettings::DeriveDisplayOrder))]
pub struct GenerateOpt {
    /// SmartModule Project Name
    name: String,

    /// Local path to generate the SmartModule project.
    /// Default to directory with project name, created in current directory
    #[clap(long, env = "SMDK_DESTINATION", value_name = "PATH")]
    destination: Option<PathBuf>,

    /// URL to git repo containing templates for generating SmartModule projects.
    /// Using this option is discouraged. The default value is recommended.
    #[clap(
        long,
        group("TemplateSourceGit"),
        conflicts_with = "TemplateSourcePath",
        value_name = "GIT_URL"
    )]
    template_repo: Option<String>,

    /// An optional git branch to use with --template-repo
    #[clap(
        long,
        group("TemplateGit"),
        requires = "TemplateSourceGit",
        value_name = "BRANCH"
    )]
    template_repo_branch: Option<String>,

    /// An optional git tag to use with --template-repo
    #[clap(
        long,
        group("TemplateGit"),
        requires = "TemplateSourceGit",
        value_name = "TAG"
    )]
    template_repo_tag: Option<String>,

    /// Local filepath containing templates for generating SmartModule projects.
    /// Using this option is discouraged. The default value is recommended.
    #[clap(
        long,
        group("TemplateSourcePath"),
        conflicts_with = "TemplateSourceGit",
        value_name = "PATH"
    )]
    template_path: Option<String>,

    /// URL of git repo to include in generated Cargo.toml. Repo used for fluvio-smartmodule dependency.
    /// Using this option is discouraged. The default value is recommended.
    #[clap(
        long,
        group("SmCrateSourceGit"),
        conflicts_with_all = &["SmCrateSourcePath", "SmCrateSourceCratesIo"],
        value_name = "GIT_URL"
    )]
    sm_crate_repo: Option<String>,

    /// An optional git branch to use with --sm-crate-repo
    #[clap(
        long,
        group("SmGit"),
        requires = "SmCrateSourceGit",
        value_name = "BRANCH"
    )]
    sm_repo_branch: Option<String>,

    /// An optional git tag to use with --sm-crate-repo
    #[clap(
        long,
        group("SmGit"),
        requires = "SmCrateSourceGit",
        value_name = "TAG"
    )]
    sm_repo_tag: Option<String>,

    /// An optional git rev to use with --sm-crate-repo
    #[clap(
        long,
        group("SmGit"),
        requires = "SmCrateSourceGit",
        value_name = "GIT_SHA"
    )]
    sm_repo_rev: Option<String>,

    /// Local filepath to include in generated Cargo.toml. Path used for fluvio-smartmodule dependency.
    /// Using this option is discouraged. The default value is recommended.
    #[clap(
        long,
        group("SmCrateSourcePath"),
        conflicts_with_all = &["SmCrateSourceGit", "SmCrateSourceCratesIo"],
        value_name = "PATH"
    )]
    sm_crate_path: Option<String>,

    /// Public version of fluvio-smartmodule from crates.io. Defaults to latest.
    /// Using this option is discouraged. The default value is recommended.
    #[clap(
        long,
        group("SmCrateSourceCratesIo"),
        conflicts_with_all = &["SmCrateSourceGit", "SmCrateSourcePath"],
        value_name = "X.Y.Z"
    )]
    sm_crate_version: Option<String>,

    /// Type of SmartModule project to generate.
    /// Skip prompt if value given.
    #[clap(long, value_enum, value_name = "TYPE")]
    sm_type: Option<SmartModuleType>,

    /// Include SmartModule state initialization function in generated SmartModule project.
    /// Skip prompt if value given.
    #[clap(long, group("SmartModuleInit"), action)]
    with_init: bool,
    /// No SmartModule state initialization function in generated SmartModule project.
    /// Skip prompt if value given.
    #[clap(long, group("SmartModuleInit"), action)]
    no_init: bool,

    /// Include SmartModule input parameters in generated SmartModule project.
    /// Skip prompt if value given.
    #[clap(long, group("SmartModuleParams"), action)]
    with_params: bool,
    /// No SmartModule input parameters in generated SmartModule project.
    /// Skip prompt if value given.
    #[clap(long, group("SmartModuleParams"), action)]
    no_params: bool,

    /// Using this option will always choose the Fluvio repo as source for templates and dependencies
    #[clap(long, action, env, conflicts_with_all = 
        &["TemplateSourceGit", "TemplateSourcePath",
        "SmCrateSourceGit", "SmCrateSourceCratesIo", "SmCrateSourcePath"],)]
    develop: bool,
}

impl GenerateOpt {
    pub(crate) fn process(self) -> Result<()> {
        println!("Generating new SmartModule project: {}", self.name);

        let init_fn = match (self.with_init, self.no_init) {
            (true, _) => Some(true),
            (_, true) => Some(false),
            _ => None,
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
            .with_smart_module_type(self.sm_type)
            .with_init_fn(init_fn)
            .with_smart_module_params(sm_params)
            .with_smart_module_cargo_dependency(Some(sm_dep_source));

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
            destination: self.destination,
            force_git_init: false,
            allow_commands: false,
            overwrite: false,
            other_args: None,
        };

        generate(args).map_err(Error::from)?;

        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq)]
enum SmdkTemplateValue {
    AddInitFn(bool),
    UseParams(bool),
    SmCargoDependency(CargoSmDependSource),
    SmType(SmartModuleType),
}

impl std::fmt::Display for SmdkTemplateValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SmdkTemplateValue::AddInitFn(init) => {
                write!(f, "smart-module-init={}", init)
            }
            SmdkTemplateValue::SmCargoDependency(dependency) => {
                write!(f, "fluvio-smartmodule-cargo-dependency={}", dependency)
            }
            SmdkTemplateValue::SmType(sm_type) => {
                write!(f, "smart-module-type={}", sm_type)
            }
            SmdkTemplateValue::UseParams(sm_params) => {
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
                git: None,
                auto_path: None,
                subfolder: None,
                test: false,
                branch: None,
                tag: None,
                path,
                favorite: None,
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
                        auto_path: None,
                        subfolder: None,
                        test: false,
                        branch: None,
                        tag: None,
                        path: None,
                        favorite: None,
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
                        auto_path: None,
                        subfolder: None,
                        test: false,
                        branch: None,
                        tag: Some(tag),
                        path: None,
                        favorite: None,
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
                        auto_path: None,
                        subfolder: None,
                        test: false,
                        branch: Some(branch),
                        tag: None,
                        path: None,
                        favorite: None,
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
                        git: None,
                        auto_path: None,
                        subfolder: None,
                        test: false,
                        branch: None,
                        tag: None,
                        path: Some(path_str),
                        favorite: None,
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

    fn with_init_fn(&mut self, request: Option<bool>) -> &mut Self {
        if let Some(i) = request {
            debug!("User provided init fn request: {i:#?}");
            self.values.push(SmdkTemplateValue::AddInitFn(i));
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
            temp_dir.find(|entry| entry.as_ref().unwrap().file_name().eq("Smart.toml"));

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
            SmdkTemplateValue::AddInitFn(true),
            SmdkTemplateValue::UseParams(true),
            SmdkTemplateValue::SmCargoDependency(CargoSmDependSource::CratesIo(
                "0.1.0".to_string(),
            )),
            SmdkTemplateValue::SmType(SmartModuleType::FilterMap),
        ];

        for value in test_template_values {
            match value {
                SmdkTemplateValue::AddInitFn(_) => {
                    assert_eq!(
                        &SmdkTemplateValue::AddInitFn(true).to_string(),
                        "smart-module-init=true"
                    );
                }
                SmdkTemplateValue::UseParams(_) => {
                    assert_eq!(
                        &SmdkTemplateValue::UseParams(true).to_string(),
                        "smart-module-params=true"
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
            .with_smart_module_cargo_dependency(Some(CargoSmDependSource::CratesIo(
                test_version_number.clone(),
            )));

        let values_vec = values.to_vec();

        for v in values_vec {
            match v {
                SmdkTemplateValue::AddInitFn(_) => {
                    assert_eq!(v, SmdkTemplateValue::AddInitFn(true));
                }

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
            }
        }
    }
}
