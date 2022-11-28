// Run Cargo using the `cargo` command line tool.

use std::{
    fmt::{Debug, Display, Formatter},
    process::{Command, Stdio},
};

use anyhow::{Error, anyhow, Result};
use derive_builder::Builder;

#[derive(Default)]
pub enum Profile {
    Debug,
    #[default]
    Release,
}

impl Display for Profile {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Profile::Debug => write!(f, "debug"),
            Profile::Release => write!(f, "release"),
        }
    }
}

/// Builder Argument
#[derive(Builder, Debug, Default)]
#[builder(setter(into))]
pub struct Cargo {
    /// Basic cargo command
    pub cmd: String,

    /// --profile
    #[builder(setter(into), default = "Profile::default().to_string()")]
    pub profile: String,
    /// --lib
    #[builder(default = "true")]
    pub lib: bool,
    /// The location at which to find the chart to install
    /// --package
    #[builder(setter(strip_option), default)]
    pub package: Option<String>,
    /// --target
    #[builder(setter(strip_option), default)]
    pub target: Option<String>,
    #[builder(default)]
    pub extra_arguments: Vec<String>,
}

impl Cargo {
    pub fn build() -> CargoBuilder {
        let mut builder = CargoBuilder::default();
        builder.cmd("build");
        builder
    }

    /// Run Cargo using the `cargo` command line tool
    pub fn run(&self) -> Result<()> {
        let mut cargo = self.make_cargo_cmd()?;

        let status = cargo.status().map_err(Error::from)?;

        if status.success() {
            Ok(())
        } else {
            let output = cargo.output()?;
            let stderr = String::from_utf8(output.stderr)?;
            Err(anyhow!(stderr))
        }
    }

    fn make_cargo_cmd(&self) -> Result<Command> {
        let cwd = std::env::current_dir()?;

        let mut cargo = Command::new("cargo");

        cargo.output().map_err(Error::from)?;
        cargo.stdout(Stdio::inherit());
        cargo.stderr(Stdio::inherit());

        cargo
            .current_dir(&cwd)
            .arg(&self.cmd)
            .arg("--profile")
            .arg(&self.profile);

        if self.lib {
            cargo.arg("--lib");
        }

        if let Some(pkg) = &self.package {
            cargo.arg("-p").arg(pkg);
        }

        if let Some(target) = &self.target {
            cargo.arg("--target").arg(target);
        }

        if !self.extra_arguments.is_empty() {
            cargo.args(&self.extra_arguments);
        }

        Ok(cargo)
    }
}

#[cfg(test)]
mod test {

    use std::ffi::OsStr;

    use super::*;

    #[test]
    fn test_builder_default() {
        let cargo = Cargo::build().build().unwrap();

        assert_eq!(cargo.profile, "release");
        assert!(cargo.lib);

        let cargo = cargo.make_cargo_cmd().expect("cmd");
        let args: Vec<&OsStr> = cargo.get_args().collect();
        assert_eq!(args, &["build", "--profile", "release", "--lib"]);
    }

    #[test]
    fn test_builder_package() {
        let config = Cargo::build().package("foo").build().expect("should build");

        assert_eq!(config.profile, "release");
        assert_eq!(config.package, Some("foo".to_string()));

        let cargo = config.make_cargo_cmd().expect("cmd");
        let args: Vec<&OsStr> = cargo.get_args().collect();
        assert_eq!(
            args,
            &["build", "--profile", "release", "--lib", "-p", "foo"]
        );
    }
}
