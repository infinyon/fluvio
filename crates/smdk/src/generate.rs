use anyhow::{Error, Result};
use clap::Parser;
use cargo_generate::{GenerateArgs, TemplatePath, generate};
use include_dir::{Dir, include_dir};
use tempdir::TempDir;

static SMART_MODULE_TEMPLATE: Dir<'static> =
    include_dir!("$CARGO_MANIFEST_DIR/../../smartmodule/cargo_template");

/// Generate new SmartModule project
#[derive(Debug, Parser)]
pub struct GenerateOpt {
    /// SmartModule Project Name
    name: String,
    /// Template to generate project from.
    ///
    /// Must be a GIT repository
    #[clap(long)]
    template: Option<String>,
}

impl GenerateOpt {
    pub(crate) fn process(self) -> Result<()> {
        println!("Generating new SmartModule project: {}", self.name);

        let template_path = if self.template.is_some() {
            TemplatePath {
                git: self.template,
                auto_path: None,
                subfolder: None,
                test: false,
                branch: None,
                tag: None,
                path: None,
                favorite: None,
            }
        } else {
            let temp_dir = TempDir::new("smartmodule_template")?;
            let path = temp_dir.path().to_str().unwrap().to_string();

            SMART_MODULE_TEMPLATE.extract(temp_dir).map_err(Error::from)?;

            TemplatePath {
                git: None,
                auto_path: None,
                subfolder: None,
                test: false,
                branch: None,
                tag: None,
                path: Some(path),
                favorite: None,
            }
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
