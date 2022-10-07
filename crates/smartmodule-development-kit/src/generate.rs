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

/// Abstraction on different of template options available for generating a
/// new SmartModule project.
///
/// May hold a reference to a `TempDir` which should not be dropped before
/// accomplishing the project generation procedure.
struct Template {
    template_path: TemplatePath,
    _temp_dir: Option<TempDir>,
}

impl Template {
    /// Extracts inlined directory contents into a temporary directory and
    /// builds a `TemplatePath` instance with the `path` pointing to the temp
    /// directory created.
    ///
    /// Is important to hold the reference to the `_temp_dir` until generation
    /// process is completed, otherwise the temp directory will be deleted
    /// before reaching the generation process.
    fn inline() -> Result<Self> {
        let temp_dir = TempDir::new("smartmodule_template")?;
        let path = temp_dir.path().to_str().unwrap().to_string();
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
                path: Some(path),
                favorite: None,
            },
            _temp_dir: Some(temp_dir),
        };

        Ok(template)
    }

    fn git(repo_uri: String) -> Result<Self> {
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
        })
    }
}

impl GenerateOpt {
    pub(crate) fn process(self) -> Result<()> {
        println!("Generating new SmartModule project: {}", self.name);

        let Template {
            template_path,
            _temp_dir,
        } = if let Some(git_uri) = self.template {
            Template::git(git_uri)?
        } else {
            Template::inline()?
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

#[cfg(test)]
mod test {
    use std::fs::read_dir;

    use super::Template;

    #[test]
    fn test_inline_template() {
        let template = Template::inline().unwrap();

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
