use anyhow::{Error, Result};
use clap::{Parser, ValueEnum, value_parser};
use cargo_generate::{GenerateArgs, TemplatePath, generate};
use include_dir::{Dir, include_dir};
use tempfile::{NamedTempFile, TempDir};
use std::io::{Write, Read};
use enum_display::EnumDisplay;
use tracing::debug;

static SMART_MODULE_TEMPLATE: Dir<'static> =
    include_dir!("$CARGO_MANIFEST_DIR/../../smartmodule/cargo_template");

#[derive(Debug, Clone)]
enum SmdkTemplateValue {
    SmartmoduleInitFn(bool),
    SmartmoduleParameters(bool),
    SmartmoduleVersion(String),
    SmartModuleType(SmartModuleType),
}

impl std::fmt::Display for SmdkTemplateValue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match &*self {
            SmdkTemplateValue::SmartmoduleInitFn(init) => {
                write!(f, "smart-module-init=\"{}\"", init)
            }
            SmdkTemplateValue::SmartmoduleVersion(version) => {
                write!(f, "smart-module-version=\"{}\"", version)
            }
            SmdkTemplateValue::SmartModuleType(sm_type) => {
                write!(f, "smart-module-type=\"{}\"", sm_type)
            }
            SmdkTemplateValue::SmartmoduleParameters(sm_params) => {
                write!(f, "smart-module-params=\"{}\"", sm_params)
            }
        }
    }
}

#[derive(ValueEnum, Clone, Debug, Parser, PartialEq, Eq, EnumDisplay)]
#[enum_display(case = "Kebab")]
enum SmartModuleType {
    Filter,
    Map,
    ArrayMap,
    Aggregate,
    FilterMap,
}

/// Generate new SmartModule project
#[derive(Debug, Parser)]
pub struct GenerateOpt {
    /// SmartModule Project Name
    name: String,

    /// URL to git repo containing the templates for generating SmartModule projects.
    /// Using this option is discouraged. The default value is recommended.
    #[clap(long)]
    smdk_template_repo: Option<String>,
    // add branch
    // add tag

    // add path
    /// Crate version or URL to `fluvio-smartmodule` git repo generated Cargo.toml.
    /// Using this option is discouraged. The default value is recommended.
    #[clap(long)]
    smart_module_crate_version: Option<String>, // maybe call this smdk_smart_module_crate

    /// Type of SmartModule project to generate.
    /// Skip prompt if value given.
    #[clap(long, value_parser = value_parser!(SmartModuleType))]
    smart_module_type: Option<SmartModuleType>,

    /// Include SmartModule state initialization function in generated SmartModule project.
    /// Skip prompt if value given.
    #[clap(long, value_parser = value_parser!(bool))]
    add_init_fn: Option<bool>,

    /// Include SmartModule input parameters in generated SmartModule project.
    /// Skip prompt if value given.
    #[clap(long, value_parser = value_parser!(bool))]
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
struct TemplateUserValuesBuilder {
    smart_module_crate_version: Option<SmdkTemplateValue>,
    smart_module_type: Option<SmdkTemplateValue>,
    generate_init_fn: Option<SmdkTemplateValue>,
    smart_module_parameters: Option<SmdkTemplateValue>,
}

impl TemplateUserValuesBuilder {
    fn new() -> Self {
        TemplateUserValuesBuilder::default()
    }

    fn with_smart_module_crate_version(&mut self, version: Option<String>) -> &mut Self {
        if let Some(v) = version {
            debug!("User provided version: {v:#?}");
            self.smart_module_crate_version = Some(SmdkTemplateValue::SmartmoduleVersion(v));
        } else {
            self.smart_module_crate_version = None;
        }
        self
    }

    fn with_smart_module_type(&mut self, sm_type: Option<SmartModuleType>) -> &mut Self {
        if let Some(t) = sm_type {
            debug!("User provided SmartModule type: {t:#?}");
            self.smart_module_type = Some(SmdkTemplateValue::SmartModuleType(t));
        } else {
            self.smart_module_type = None;
        }
        self
    }

    fn with_init_fn(&mut self, request: Option<bool>) -> &mut Self {
        if let Some(i) = request {
            debug!("User provided init fn request: {i:#?}");
            self.generate_init_fn = Some(SmdkTemplateValue::SmartmoduleInitFn(i));
        } else {
            self.generate_init_fn = None;
        }
        self
    }

    fn with_smart_module_params(&mut self, request: Option<bool>) -> &mut Self {
        if let Some(i) = request {
            debug!("User provided SmartModule params request: {i:#?}");
            self.smart_module_parameters = Some(SmdkTemplateValue::SmartmoduleParameters(i));
        } else {
            self.smart_module_parameters = None;
        }
        self
    }

    fn build(&self) -> Result<Option<TemplateUserValues>> {
        debug!("Generating values file");
        if self.is_user_input() {
            let mut values_file = TemplateUserValues::new()?;

            if let Some(v) = &self.smart_module_crate_version {
                values_file.append_value(&v)?;
            }

            if let Some(v) = &self.smart_module_type {
                values_file.append_value(&v)?;
            }

            if let Some(v) = &self.generate_init_fn {
                values_file.append_value(&v)?;
            }

            if let Some(v) = &self.smart_module_parameters {
                values_file.append_value(&v)?;
            }

            Ok(Some(values_file))
        } else {
            Ok(None)
        }
    }

    // Did the user provide any values?
    fn is_user_input(&self) -> bool {
        self.smart_module_crate_version.is_some()
            || self.smart_module_type.is_some()
            || self.generate_init_fn.is_some()
            || self.smart_module_parameters.is_some()
    }
}

#[derive(Debug)]
struct TemplateUserValues {
    tempfile: NamedTempFile,
}

impl TemplateUserValues {
    fn new() -> Result<Self> {
        debug!("Creating values tempfile and writing header");
        let mut tempfile = NamedTempFile::new()?;

        writeln!(tempfile, "[values]")?;
        tempfile.flush()?;

        Ok(Self { tempfile })
    }

    fn append_value(&mut self, user_value: &SmdkTemplateValue) -> Result<()> {
        debug!("Writing to values file: {user_value}");
        writeln!(self.tempfile, "{}", user_value)?;
        self.tempfile.flush()?;
        Ok(())
    }

    fn path(&self) -> Option<String> {
        self.tempfile
            .path()
            .to_path_buf()
            .to_str()
            .map(|v| v.to_string())
    }

    fn print_file(&self) -> Result<()> {
        debug!("Printing the values file to stdout");
        let mut values_file = self.tempfile.reopen()?;
        let mut content = String::new();

        values_file.read_to_string(&mut content)?;

        println!("{content}");

        Ok(())
    }
}

impl GenerateOpt {
    pub(crate) fn process(self) -> Result<()> {
        println!("Generating new SmartModule project: {}", self.name);

        let maybe_user_input: Option<TemplateUserValues>;

        let SmdkTemplate {
            template_path,
            _temp_dir,
            _template_source,
        } = if let Some(git_uri) = self.smdk_template_repo {
            maybe_user_input = TemplateUserValuesBuilder::new()
                .with_smart_module_crate_version(self.smart_module_crate_version)
                .with_smart_module_type(self.smart_module_type)
                .with_init_fn(self.add_init_fn)
                .with_smart_module_params(self.smart_module_params)
                .build()?;
            SmdkTemplate::git(git_uri)?
        } else {
            // FIXME: This should not default to git repo
            let sm_version = "git = \\\"https://github.com/infinyon/fluvio.git\\\"".to_string();

            maybe_user_input = TemplateUserValuesBuilder::new()
                .with_smart_module_crate_version(Some(sm_version))
                .with_smart_module_type(self.smart_module_type)
                .with_init_fn(self.add_init_fn)
                .with_smart_module_params(self.smart_module_params)
                .build()?;
            SmdkTemplate::default()?
        };

        let maybe_values_file: Option<String> = if let Some(input) = &maybe_user_input {
            input.print_file()?;
            input.path()
        } else {
            None
        };

        let args = GenerateArgs {
            template_path,
            name: Some(self.name.clone()),
            list_favorites: false,
            force: false,
            verbose: true,
            template_values_file: maybe_values_file,
            silent: false,
            config: None,
            vcs: None,
            lib: false,
            bin: false,
            ssh_identity: None,
            define: Vec::default(),
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
}
