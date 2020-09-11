use std::io::Error as IoError;
use std::io::ErrorKind;
use std::process::{Command, Stdio};

use serde::Deserialize;
use flv_util::cmd::CommandExt;

#[derive(Debug)]
#[non_exhaustive]
pub struct HelmClient {}

impl HelmClient {
    /// Creates a Rust client to manage our helm needs.
    ///
    /// This only succeeds if the helm command can be found.
    pub fn new() -> Result<Self, IoError> {
        let output = Command::new("helm").arg("version").print().output()?;

        // Convert command output into a string
        let out_str = String::from_utf8(output.stdout).map_err(|e| {
            IoError::new(
                ErrorKind::InvalidData,
                format!("failed to parse helm output as string: {}", e),
            )
        })?;

        // Check that the version command gives a version.
        // In the future, we can parse the version string and check
        // for compatible CLI client version.
        if !out_str.contains("version") {
            return Err(IoError::new(
                ErrorKind::InvalidData,
                "failed to get helm version",
            ));
        }

        // If checks succeed, create Helm client
        Ok(Self {})
    }

    /// Installs the given chart under the given name.
    ///
    /// The `opts` are passed to helm as `--set` arguments.
    pub fn install(
        &self,
        namespace: &str,
        name: &str,
        chart: &str,
        version: Option<&str>,
        opts: &[(&str, &str)],
    ) -> Result<(), IoError> {
        let sets: Vec<_> = opts
            .iter()
            .flat_map(|(key, val)| vec!["--set".to_string(), format!("{}={}", key, val)])
            .collect();

        let mut command = Command::new("helm");
        command
            .args(&["install", name, chart])
            .args(&["--namespace", namespace])
            .args(sets);

        if let Some(version) = version {
            command.args(&["--version", version]);
        }

        command.inherit();
        Ok(())
    }

    /// Adds a new helm repo with the given chart name and chart location
    pub fn repo_add(&self, chart: &str, location: &str) -> Result<(), IoError> {
        Command::new("helm")
            .args(&["repo", "add", chart, location])
            .stdout(Stdio::inherit())
            .stdout(Stdio::inherit())
            .inherit();
        Ok(())
    }

    /// Updates the local helm repository
    pub fn repo_update(&self) -> Result<(), IoError> {
        Command::new("helm").args(&["repo", "update"]).inherit();
        Ok(())
    }

    /// Searches the repo for the named helm chart
    pub fn search_repo(&self, chart: &str, version: &str) -> Result<Vec<Chart>, IoError> {
        let output = Command::new("helm")
            .args(&["search", "repo", chart])
            .args(&["--version", version])
            .args(&["--output", "json"])
            .print()
            .output()?;

        serde_json::from_slice(&output.stdout).map_err(|_| {
            IoError::new(
                ErrorKind::InvalidData,
                "failed to parse helm chart versions",
            )
        })
    }

    /// Get all the available versions
    pub fn get_versions(&self, chart: &str) -> Result<Vec<Chart>, IoError> {
        let output = Command::new("helm")
            .args(&["search", "repo"])
            .args(&["--versions", chart])
            .args(&["--output", "json"])
            .print()
            .output()?;

        serde_json::from_slice(&output.stdout).map_err(|_| {
            IoError::new(
                ErrorKind::InvalidData,
                "failed to fetch helm chart versions",
            )
        })
    }

    /// Checks that a given version of a given chart exists in the repo.
    pub fn chart_version_exists(&self, name: &str, version: &str) -> Result<bool, IoError> {
        let versions = self.search_repo(name, version)?;
        let count = versions
            .iter()
            .filter(|chart| chart.name == name && chart.version == version)
            .count();
        Ok(count > 0)
    }
}

/// A representation of a chart definition in a repo.
#[derive(Debug, Deserialize)]
pub struct Chart {
    /// The chart name
    pub name: String,
    /// The chart version
    pub version: String,
}

/// A representation of an installed chart.
#[derive(Debug, Deserialize)]
pub struct InstalledChart {
    /// The chart name
    pub name: String,
    /// The version of the app this chart installed
    pub app_version: String,
}
