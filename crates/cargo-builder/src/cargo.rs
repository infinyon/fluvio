//! Run Cargo using the `cargo` command line tool.

use std::{
    fmt::{Debug, Display, Formatter},
    process::{Command, Stdio},
};

use anyhow::{Error, anyhow, Result};
use derive_builder::Builder;

/// Error returned from `CargoBuild` builder when both the `target` field and
/// an extra argument with `--target` are both present.
const AMBIGUOUS_TARGET_ERR_MSG: &str =
    "Cannot use `--target` as extra argument if `target` is also provided";

const BUILD_CMD: &str = "build";
const CLEAN_CMD: &str = "clean";
const ZIGBUILD_CMD: &str = "zigbuild";

#[derive(Default)]
pub enum Profile {
    Debug,
    #[default]
    Release,
}

#[derive(Default, Clone, Debug)]
pub enum CargoCommand {
    #[default]
    Build,
    Clean,
    ZigBuild,
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
#[builder(build_fn(validate = "Self::validate"))]
pub struct Cargo {
    /// Basic cargo command
    pub cmd: CargoCommand,

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
        builder.cmd(CargoCommand::Build);
        builder
    }

    pub fn clean() -> CargoBuilder {
        let mut builder = CargoBuilder::default();
        builder.lib(false);
        builder.cmd(CargoCommand::Clean);
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

        match &self.cmd {
            CargoCommand::Build => {
                cargo
                    .current_dir(&cwd)
                    .arg(BUILD_CMD)
                    .arg("--profile")
                    .arg(&self.profile);
            }
            CargoCommand::Clean => {
                cargo.current_dir(&cwd).arg(CLEAN_CMD);
            }
            CargoCommand::ZigBuild => {
                cargo
                    .current_dir(&cwd)
                    .arg(ZIGBUILD_CMD)
                    .arg("--profile")
                    .arg(&self.profile);
            }
        }

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

impl CargoBuilder {
    fn validate(&self) -> Result<(), String> {
        if let (Some(ref _target), Some(ref extra_arguments)) =
            (&self.target, &self.extra_arguments)
        {
            // We don't want to allow setting both `target` and also
            // `extra_arguments` containing `--target` in it
            if extra_arguments.contains(&"--target".to_string()) {
                return Err(AMBIGUOUS_TARGET_ERR_MSG.to_string());
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use std::ffi::OsStr;

    use super::*;
    const WASM_TARGET: &str = "wasm32-wasi";

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

    #[test]
    fn test_builder_build() {
        let config = Cargo::build().build().expect("should build");
        assert!(matches!(config.cmd, CargoCommand::Build));

        let cargo = config.make_cargo_cmd().expect("cmd");
        let args: Vec<&OsStr> = cargo.get_args().collect();
        assert_eq!(args, &["build", "--profile", "release", "--lib"]);
    }

    #[test]
    fn test_builder_clean() {
        let config = Cargo::clean().build().expect("should build");
        assert!(matches!(config.cmd, CargoCommand::Clean));

        let cargo = config.make_cargo_cmd().expect("cmd");
        let args: Vec<&OsStr> = cargo.get_args().collect();
        assert_eq!(args, &["clean"]);
    }

    #[test]
    fn test_builder_target() {
        let config = Cargo::build()
            .package("foo")
            .target(WASM_TARGET)
            .build()
            .expect("should build");

        assert_eq!(config.profile, "release");
        assert_eq!(config.package, Some("foo".to_string()));

        let cargo = config.make_cargo_cmd().expect("cmd");
        let args: Vec<&OsStr> = cargo.get_args().collect();
        assert_eq!(
            args,
            &[
                "build",
                "--profile",
                "release",
                "--lib",
                "-p",
                "foo",
                "--target",
                WASM_TARGET
            ]
        );
    }

    #[test]
    fn test_builder_extra_args() {
        let config = Cargo::build()
            .package("foo")
            .target(WASM_TARGET)
            .extra_arguments(vec![
                "--benches".to_string(),
                "--no-default-features".to_string(),
            ])
            .build()
            .expect("should build");

        assert_eq!(config.profile, "release");
        assert_eq!(config.package, Some("foo".to_string()));

        let cargo = config.make_cargo_cmd().expect("cmd");
        let args: Vec<&OsStr> = cargo.get_args().collect();
        assert_eq!(
            args,
            &[
                "build",
                "--profile",
                "release",
                "--lib",
                "-p",
                "foo",
                "--target",
                WASM_TARGET,
                "--benches",
                "--no-default-features"
            ]
        );
    }

    #[test]
    fn test_builder_complains_extra_args_target() {
        let config = Cargo::build()
            .package("foo")
            .target(WASM_TARGET)
            .extra_arguments(vec![
                "--benches".to_string(),
                "--no-default-features".to_string(),
                "--target".to_string(),
                "aarch64-apple-darwin".to_string(),
            ])
            .build();

        assert!(config.is_err());

        let Err(err) = config else {
            panic!("Expected a `Err` for `test_builder_complains_extra_args_target`");
        };

        assert_eq!(err.to_string(), AMBIGUOUS_TARGET_ERR_MSG);
    }
}
