use std::io::Error as IoError;
use std::string::FromUtf8Error;
use std::process::{Command, Stdio};

use tracing::{instrument, warn};
use thiserror::Error;
use serde::Deserialize;
use flv_util::cmd::CommandExt;

#[derive(Error, Debug)]
pub enum HelmError {
    #[error(
        r#"Unable to find 'helm' executable
  Please make sure helm is installed and in your PATH.
  See https://helm.sh/docs/intro/install/ for more help"#
    )]
    HelmNotInstalled {
        #[from]
        source: IoError,
    },
    #[error("failed to read helm client version: {0}")]
    HelmVersionNotFound(String),
    #[error("failed to parse helm output as UTF8")]
    Utf8Error {
        #[from]
        source: FromUtf8Error,
    },
    #[error("failed to parse JSON from helm output")]
    Serde {
        #[from]
        source: serde_json::Error,
    },
}

/// Client to manage helm operations
#[derive(Debug)]
#[non_exhaustive]
pub struct HelmClient {}

impl HelmClient {
    /// Creates a Rust client to manage our helm needs.
    ///
    /// This only succeeds if the helm command can be found.
    pub fn new() -> Result<Self, HelmError> {
        let output = Command::new("helm")
            .arg("version")
            .print()
            .output()
            .map_err(|source| HelmError::HelmNotInstalled { source })?;

        // Convert command output into a string
        let out_str =
            String::from_utf8(output.stdout).map_err(|source| HelmError::Utf8Error { source })?;

        // Check that the version command gives a version.
        // In the future, we can parse the version string and check
        // for compatible CLI client version.
        if !out_str.contains("version") {
            return Err(HelmError::HelmVersionNotFound(out_str));
        }

        // If checks succeed, create Helm client
        Ok(Self {})
    }

    /// Installs the given chart under the given name.
    ///
    /// The `opts` are passed to helm as `--set` arguments.
    #[instrument(skip(self, version, opts))]
    pub(crate) fn install(
        &self,
        namespace: &str,
        name: &str,
        chart: &str,
        version: Option<&str>,
        opts: &[(&str, &str)],
    ) -> Result<(), HelmError> {
        let sets: Vec<_> = opts
            .iter()
            .flat_map(|(key, val)| vec!["--set".to_string(), format!("{}={}", key, val)])
            .collect();

        let mut command = Command::new("helm");
        command
            .args(&["install", name, chart])
            .args(&["--namespace", namespace])
            .args(&["--devel"])
            .args(sets);

        if let Some(version) = version {
            command.args(&["--version", version]);
        }

        command.inherit();
        Ok(())
    }

    /// Uninstalls specified chart library
    pub(crate) fn uninstall(&self, name: &str, ignore_not_found: bool) -> Result<(), HelmError> {
        if ignore_not_found {
            let app_charts = self.get_installed_chart_by_name(name)?;
            if app_charts.is_empty() {
                warn!("Chart does not exists, {}", &name);
                return Ok(());
            }
        }
        let mut command = Command::new("helm");
        command.args(&["uninstall", name]);

        command.inherit();
        Ok(())
    }

    /// Adds a new helm repo with the given chart name and chart location
    #[instrument(skip(self))]
    pub(crate) fn repo_add(&self, chart: &str, location: &str) -> Result<(), HelmError> {
        Command::new("helm")
            .args(&["repo", "add", chart, location])
            .stdout(Stdio::inherit())
            .stdout(Stdio::inherit())
            .inherit();
        Ok(())
    }

    /// Updates the local helm repository
    #[instrument(skip(self))]
    pub(crate) fn repo_update(&self) -> Result<(), HelmError> {
        Command::new("helm").args(&["repo", "update"]).inherit();
        Ok(())
    }

    /// Searches the repo for the named helm chart
    #[instrument(skip(self))]
    pub(crate) fn search_repo(&self, chart: &str, version: &str) -> Result<Vec<Chart>, HelmError> {
        let output = Command::new("helm")
            .args(&["search", "repo", chart])
            .args(&["--version", version])
            .args(&["--output", "json"])
            .print()
            .output()?;

        serde_json::from_slice(&output.stdout).map_err(|source| HelmError::Serde { source })
    }

    /// Get all the available versions
    #[instrument(skip(self))]
    pub(crate) fn versions(&self, chart: &str) -> Result<Vec<Chart>, HelmError> {
        let output = Command::new("helm")
            .args(&["search", "repo"])
            .args(&["--versions", chart])
            .args(&["--output", "json", "--devel"])
            .print()
            .output()?;

        serde_json::from_slice(&output.stdout).map_err(|source| HelmError::Serde { source })
    }

    /// Checks that a given version of a given chart exists in the repo.
    #[instrument(skip(self))]
    pub(crate) fn chart_version_exists(
        &self,
        name: &str,
        version: &str,
    ) -> Result<bool, HelmError> {
        let versions = self.search_repo(name, version)?;
        let count = versions
            .iter()
            .filter(|chart| chart.name == name && chart.version == version)
            .count();
        Ok(count > 0)
    }

    /// Returns the list of installed charts by name
    #[instrument(skip(self))]
    pub(crate) fn get_installed_chart_by_name(
        &self,
        name: &str,
    ) -> Result<Vec<InstalledChart>, HelmError> {
        let exact_match = format!("^{}$", name);
        let output = Command::new("helm")
            .arg("list")
            .arg("--filter")
            .arg(exact_match)
            .arg("--output")
            .arg("json")
            .print()
            .output()?;

        serde_json::from_slice(&output.stdout).map_err(|source| HelmError::Serde { source })
    }

    /// get helm package version
    #[instrument(skip(self))]
    pub(crate) fn get_helm_version(&self) -> Result<String, HelmError> {
        let helm_version = Command::new("helm")
            .arg("version")
            .arg("--short")
            .output()
            .map_err(|source| HelmError::HelmNotInstalled { source })?;
        let version_text = String::from_utf8(helm_version.stdout)
            .map_err(|source| HelmError::Utf8Error { source })?;
        Ok(version_text[1..].trim().to_string())
    }
}

/// A representation of a chart definition in a repo.
#[derive(Debug, Deserialize)]
pub struct Chart {
    /// The chart name
    name: String,
    /// The chart version
    version: String,
}

impl Chart {
    pub fn version(&self) -> &str {
        &self.version
    }
    pub fn name(&self) -> &str {
        &self.name
    }
}

/// A representation of an installed chart.
#[derive(Debug, Deserialize)]
pub struct InstalledChart {
    /// The chart name
    pub name: String,
    /// The version of the app this chart installed
    pub app_version: String,
}
