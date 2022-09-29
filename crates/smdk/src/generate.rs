use anyhow::{Error, Result};
use clap::Parser;
use cargo_generate::{GenerateArgs, TemplatePath, generate};

const GIT_TEMPLATE: &str =
    "https://github.com/infinyon/fluvio/tree/master/smartmodule/cargo_template";

/// Generate new SmartModule project
#[derive(Debug, Parser)]
pub struct GenerateOpt {
    name: String,
}

impl GenerateOpt {
    pub(crate) fn process(&self) -> Result<()> {
        println!("Generating new SmartModule project: {}", self.name);
        let template_path = TemplatePath {
            git: Some(String::from(GIT_TEMPLATE)),
            auto_path: None,
            subfolder: None,
            test: false,
            branch: None,
            tag: None,
            path: None,
            favorite: None,
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
