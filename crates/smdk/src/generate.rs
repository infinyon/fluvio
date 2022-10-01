use anyhow::{Error, Result};
use clap::Parser;
use cargo_generate::{GenerateArgs, TemplatePath, generate};
use include_dir::{Dir, include_dir};

static SMART_MODULE_TEMPLATE: Dir<'_> =
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

        let template_path = match self.template {
            Some(git) => TemplatePath {
                git: Some(git),
                auto_path: None,
                subfolder: None,
                test: false,
                branch: None,
                tag: None,
                path: None,
                favorite: None,
            },
            None => {
                let path = SMART_MODULE_TEMPLATE.path().to_str().unwrap().to_string();

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
