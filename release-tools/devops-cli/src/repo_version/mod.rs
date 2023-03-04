use std::fs;
use std::fs::File;
use std::io::{Write};

use semver::{Version, Prerelease, BuildMetadata};
use anyhow::{Result, anyhow};
use clap::{Parser, ValueEnum};
use tracing::{info, error};
use serde::{Serialize, Deserialize};
use strum::{EnumString, EnumVariantNames, Display, VariantNames};

const VERSION: &str = "VERSION";
const DEFAULT_PRERELEASE_ITER: u32 = 1;

/// Dev.n -> Alpha.n -> Beta.n -> Release -> Dev.n ...
#[derive(
    Debug,
    Parser,
    Clone,
    ValueEnum,
    PartialEq,
    Serialize,
    Deserialize,
    EnumString,
    Display,
    Default,
    EnumVariantNames,
)]
#[strum(serialize_all = "lowercase")]
#[clap(rename_all = "lowercase")]
pub enum VersionBump {
    None,
    /// `x.y.z-dev.(# + 1)`
    #[default]
    Dev,
    /// `x.y.z-alpha.(# + 1)`
    Alpha,
    /// `x.y.z-beta.(# + 1)`
    Beta,
    /// `x.y.z`
    Release,
    /// `x.y.(z + 1)`
    Patch,
    /// `x.(y + 1).z`
    Minor,
    /// `(x + 1).y.z`
    Major,
}

#[derive(Debug, Parser)]
pub struct UpdateVersionOpt {
    #[clap(value_enum, group = "version", default_value = "none")]
    bump: VersionBump,
    #[clap(long, group = "version")]
    set: Option<String>,
}

impl UpdateVersionOpt {
    pub fn execute(&self) -> Result<()> {
        if let Some(version) = &self.set {
            info!("Write version {version} to {VERSION}");

            let parsed: Version = version.clone().parse()?;

            if self.validate_prerelease(&parsed.pre) {
                let path = VERSION;
                let mut output = File::create(path)?;
                let line = version.to_string();

                info!("Next version: {line}");
                write!(output, "{line}")?;

                return Ok(());
            } else {
                return Err(anyhow!(
                    "Prerelease is not in format: <stage>.<#iter> - ex alpha.1"
                ));
            }
        }

        let version_str = fs::read_to_string(VERSION)?;
        let version: Version = version_str.parse()?;

        info!("Current: {version:?}");

        if self.bump == VersionBump::None && self.set.is_none() {
            return Err(anyhow!(
                "Choose a version bump type {:?} or use --set <semver>",
                VersionBump::VARIANTS
            ));
        }

        let next = self.update_version(version)?;

        let path = VERSION;
        let mut output = File::create(path)?;
        let line = format!("{next}");
        write!(output, "{line}")?;

        Ok(())
    }

    /// Calculate the version based on `VersionBump`
    fn update_version(&self, version: Version) -> Result<Version> {
        let next = match &self.bump {
            VersionBump::Major => Version {
                major: version.major + 1,
                minor: version.minor,
                patch: version.patch,
                pre: Prerelease::EMPTY,
                build: BuildMetadata::EMPTY,
            },
            VersionBump::Minor => Version {
                major: version.major,
                minor: version.minor + 1,
                patch: version.patch,
                pre: Prerelease::EMPTY,
                build: BuildMetadata::EMPTY,
            },
            VersionBump::Patch => Version {
                major: version.major,
                minor: version.minor,
                patch: version.patch + 1,
                pre: Prerelease::EMPTY,
                build: BuildMetadata::EMPTY,
            },
            VersionBump::Release => Version {
                major: version.major,
                minor: version.minor,
                patch: version.patch,
                pre: Prerelease::EMPTY,
                build: BuildMetadata::EMPTY,
            },
            _ => Version {
                major: version.major,
                minor: version.minor,
                patch: self.prerelease_patch(&version)?,
                pre: self.next_prerelease(&version)?,
                build: BuildMetadata::EMPTY,
            },
        };

        info!("Next version: {next}");
        Ok(next)
    }

    /// Returns the patch version depending on whether we're currently in a prerelease or a stable release.
    fn prerelease_patch(&self, version: &Version) -> Result<u64> {
        let patch = if version.pre.is_empty() {
            version.patch + 1
        } else {
            version.patch
        };

        Ok(patch)
    }

    /// Loosely checks that our naming conventions are being followed.
    /// Will accept form with `-` instead of `.` (i.e., `<dev|alpha|beta>-#`).
    ///
    /// If you use `--set` and use `-`, it will be written.
    /// But any prerelease bump will correct it to use `.`
    fn validate_prerelease(&self, pre: &Prerelease) -> bool {
        if pre.is_empty() {
            true
        } else {
            let pre = pre.as_str();
            let current: Vec<&str> = pre.split(['.', '-']).collect();
            info!("stage: {}", &current[0]);

            if !current.is_empty() && current.len() != 2 {
                error!("{} is not in format: <stage>.<#iter> - ex alpha.1", pre);
                false
            } else if !VersionBump::VARIANTS.contains(&current[0]) {
                error!("{} is not one of our prerelease stages", &current[0]);
                false
            } else {
                info!("Prerelease acceptable format: {}", pre);
                true
            }
        }
    }

    /// Increase the iteration of the prerelease stage, if the stage matches
    ///
    /// If we're on a release version (i.e., the current version has no prerelease, ex. `0.10.5`),
    /// the next bump will also increase minor number.
    ///
    /// So a "dev" bump from `0.10.5` will be `0.10.6-dev.1`
    ///
    /// If the file version uses `-` as a iteration separator,
    /// this will enforce using `.` so `semver` can use comparison operators (`<`, `>`, etc.)
    fn next_prerelease(&self, version: &Version) -> Result<Prerelease> {
        if !(self.bump == VersionBump::Dev
            || self.bump == VersionBump::Alpha
            || self.bump == VersionBump::Beta)
        {
            return Ok(Prerelease::EMPTY);
        }

        if version.pre.is_empty() {
            Ok(default_prerelease()?)
        } else {
            if !self.validate_prerelease(&version.pre) {
                return Err(anyhow!(
                    "Prerelease is not in format: <stage>.<#iter> - ex alpha.1"
                ));
            }

            let current: Vec<&str> = version.pre.as_str().split(['.', '-']).collect();

            let current_stage = match current[0] {
                "dev" => VersionBump::Dev,
                "alpha" => VersionBump::Alpha,
                "beta" => VersionBump::Beta,
                found => return Err(anyhow!("Unsupported prerelease stage: {found}")),
            };
            let current_iter: u32 = current[1].parse()?;
            info!("Iter: {current_iter}");

            let next_prerelease = if self.bump != current_stage {
                format!("{}.{}", self.bump, DEFAULT_PRERELEASE_ITER)
            } else {
                format!("{}.{}", self.bump, current_iter + 1)
            };
            info!("Next prerelease: {next_prerelease}");

            Ok(Prerelease::new(&next_prerelease)?)
        }
    }
}

fn default_prerelease() -> Result<Prerelease> {
    let default_pre = format!("{}.{}", VersionBump::default(), DEFAULT_PRERELEASE_ITER);
    Ok(Prerelease::new(&default_pre)?)
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_prerelease() -> Result<()> {
        let opt = UpdateVersionOpt {
            bump: VersionBump::None,
            set: None,
        };

        // Pass
        assert!(opt.validate_prerelease(&Version::parse("0.1.0-dev-1")?.pre));
        assert!(opt.validate_prerelease(&Version::parse("0.1.0-alpha.0")?.pre));
        assert!(opt.validate_prerelease(&Version::parse("0.2.0-beta.6542")?.pre));

        // fail
        assert!(!opt.validate_prerelease(&Version::parse("0.6.7-gamma.1")?.pre));
        assert!(!opt.validate_prerelease(&Version::parse("0.6.7-alpha")?.pre));

        Ok(())
    }

    #[test]
    fn test_next_prerelease() -> Result<()> {
        let dev = UpdateVersionOpt {
            bump: VersionBump::Dev,
            set: None,
        };

        assert_eq!(
            dev.next_prerelease(&Version::parse("0.1.0-dev-1")?)?
                .as_str(),
            "dev.2"
        );

        assert_eq!(
            dev.next_prerelease(&Version::parse("0.1.0-dev.2")?)?
                .as_str(),
            "dev.3"
        );

        let beta = UpdateVersionOpt {
            bump: VersionBump::Beta,
            set: None,
        };

        assert_eq!(
            beta.next_prerelease(&Version::parse("0.1.0-dev.3")?)?
                .as_str(),
            "beta.1"
        );

        assert_eq!(
            dev.next_prerelease(&Version::parse("0.1.0-beta.1")?)?
                .as_str(),
            "dev.1"
        );

        let release = UpdateVersionOpt {
            bump: VersionBump::Release,
            set: None,
        };

        assert_eq!(
            release
                .next_prerelease(&Version::parse("0.1.0-dev.1")?)?
                .as_str(),
            ""
        );

        assert_eq!(
            dev.next_prerelease(&Version::parse("0.1.0")?)?.as_str(),
            "dev.1"
        );

        Ok(())
    }

    #[test]
    fn test_update_version() -> Result<()> {
        let dev = UpdateVersionOpt {
            bump: VersionBump::Dev,
            set: None,
        };

        assert_eq!(
            dev.update_version(Version::parse("0.1.0")?)?,
            Version::parse("0.1.1-dev.1")?
        );
        assert_eq!(
            dev.update_version(Version::parse("0.1.1-dev.1")?)?,
            Version::parse("0.1.1-dev.2")?
        );

        let release = UpdateVersionOpt {
            bump: VersionBump::Release,
            set: None,
        };

        assert_eq!(
            release.update_version(Version::parse("0.1.1-dev.1")?)?,
            Version::parse("0.1.1")?
        );

        Ok(())
    }
}
