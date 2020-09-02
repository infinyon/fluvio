use std::process::Command;

use tracing::debug;
use serde::Deserialize;

use super::*;

pub fn repo_add(chart_location: Option<&str>) {
    // add repo
    Command::new("helm")
        .arg("repo")
        .arg("add")
        .arg("fluvio")
        .arg(chart_location.unwrap_or("https://infinyon.github.io"))
        .inherit();
}

pub fn repo_update() {
    // add repo
    Command::new("helm").arg("repo").arg("update").inherit();
}

#[derive(Debug, Deserialize)]
struct Chart {
    name: String,
    version: String,
}

#[derive(Debug, Deserialize)]
pub struct SysChart {
    pub name: String,
    pub app_version: String,
}

pub fn installed_sys_charts(name: &str) -> Vec<SysChart> {
    let mut cmd = Command::new("helm");
    cmd.arg("list")
        .arg("--filter")
        .arg(name)
        .arg("--output")
        .arg("json")
        .print();

    debug!("command {:?}", cmd);

    let output = cmd.output().expect("unable to fetch helm list");
    debug!("command output {:?}", output);
    serde_json::from_slice(&output.stdout).expect("invalid json from helm list")
}
