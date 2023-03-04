use std::fs;
use std::path::PathBuf;

use anyhow::{Result};
use subprocess::{Exec, Redirection};
use semver::{Version, Prerelease, BuildMetadata};
use clap::Parser;
use tracing::info;

#[derive(Debug, Parser)]
pub struct UpdateChangelogOpt {
    /// Path to git-cliff config
    #[clap(long, default_value = "./cliff.toml")]
    git_cliff_config: PathBuf,

    /// Release version to use. Defaults to VERSION
    #[clap(long)]
    release_version: Option<String>,

    /// Previous version to use. Defaults to previous stable patch version
    #[clap(long)]
    prev_version: Option<String>,

    /// Git commit range start for git-cliff. Defaults to previous stable
    #[clap(long)]
    range_start: Option<String>,

    /// Git commit range end for git-cliff. Defaults to HEAD
    #[clap(long)]
    range_end: Option<String>,

    /// Target path to generated changelog
    #[clap(long, default_value = "CHANGELOG-generated.md", group = "output")]
    changelog_out: PathBuf,

    /// Generate output as json to stdout
    #[clap(long, action, group = "output")]
    json: bool,

    /// Print out debugging info
    #[clap(long, action, group = "output")]
    verbose: bool,
}

impl UpdateChangelogOpt {
    pub fn execute(&self) -> Result<()> {
        let version_str = self.get_target_version()?;
        let version: Version = version_str.parse()?;
        let previous_stable = self.get_previous_stable(&version)?;
        let (range_start, range_end) = self.get_commit_range(&previous_stable);
        let tag_range = format!("{range_start}..{range_end}");
        let changelog_path: String = self.changelog_out.display().to_string();
        let git_cliff_config = self.git_cliff_config.display().to_string();

        let cmd = "git";
        let mut args = vec![
            "cliff",
            "--config",
            &git_cliff_config,
            "--tag",
            &version_str,
            &tag_range,
        ];

        if self.json {
            args.push("--context")
        } else {
            args.push("-o");
            args.push(&changelog_path);
        }

        // We want to be able to pipe json output to `jq`,
        if self.verbose {
            info!("Previous: v{previous_stable}");
            info!("Current v{version}");
            info!("Range: {tag_range}");
            info!("Output file: {changelog_path}");
            info!("Git-cliff config: {git_cliff_config}");

            args.push("--verbose");
        }

        if !self.json {
            info!("Generating changelog {tag_range}")
        }

        let stream = Exec::cmd(cmd)
            .args(&args)
            .stdout(Redirection::Pipe)
            .stderr(Redirection::Merge)
            .capture()?
            .stdout_str();

        println!("{stream}");

        Ok(())
    }

    /// Return the release version of the previous patch release, unless `--prev-version` is used
    fn get_previous_stable(&self, version: &Version) -> Result<Version> {
        let previous_stable: Version = if let Some(prev) = &self.prev_version {
            prev.parse()?
        } else {
            Version {
                major: version.major,
                minor: version.minor,
                patch: version.patch - 1,
                pre: Prerelease::EMPTY,
                build: BuildMetadata::EMPTY,
            }
        };
        Ok(previous_stable)
    }

    /// Read the version from VERSION file, unless `--release-version` used
    fn get_target_version(&self) -> Result<String> {
        let version_str: String = if let Some(v) = &self.release_version {
            v.to_string()
        } else {
            fs::read_to_string("VERSION")?
        };
        Ok(version_str)
    }

    /// Returns range of `v<current-release - 1>..HEAD`,
    /// unless we override with `--range-start` or `--range-end`
    fn get_commit_range(&self, previous_stable: &Version) -> (String, String) {
        let range_start = if let Some(start) = &self.range_start {
            start.to_string()
        } else {
            format!("v{previous_stable}")
        };

        let range_end = if let Some(end) = &self.range_end {
            end.to_string()
        } else {
            "HEAD".to_string()
        };

        (range_start, range_end)
    }
}
