use std::{cmp::max, collections::HashMap, fs, path::PathBuf, process::Command};

use serde::Deserialize;
use toml_diff::TomlDiff;

mod download;

use download::download_crate;

const PUBLISH_LIST_PATH: &str = "./publish-list.toml";
const CRATES_DIR: &str = "../../crates";
const CRATES_IO_DIR: &str = "./crates_io";

enum CrateStatus {
    NotPublished,
    VersionBumped,
    Unchanged,
    CodeChanged,
    ManifestChanged(String),
}

#[tokio::main]
async fn main() {
    let publish_list = read_publish_list();
    let padding = publish_list
        .iter()
        .fold(0, |len, name| max(len, name.len()));

    let status_checks: Vec<_> = publish_list
        .into_iter()
        .map(|crate_name| {
            (
                crate_name.clone(),
                tokio::spawn(check_crate_status(crate_name)),
            )
        })
        .collect();
    let mut crate_status: HashMap<String, CrateStatus> =
        HashMap::with_capacity(status_checks.len());

    for status_check in status_checks {
        let (name, status) = status_check;
        let status = status.await.unwrap();
        crate_status.insert(name, status);
    }
    for (crate_name, status) in crate_status.into_iter() {
        match status {
            CrateStatus::VersionBumped => println!("🟢 {crate_name:-padding$} Version number has been updated"),
            CrateStatus::Unchanged => println!("🟢 {crate_name:-padding$} Code does not differ from crates.io"),
            CrateStatus::CodeChanged => {
                println!("⛔ {crate_name:-padding$} Code changed but version number did not:");
                diff_crate_src(&crate_name, false);
            },
            CrateStatus::ManifestChanged(diff) => {
                println!("⛔ {crate_name:-padding$} Manifest (Cargo.toml) changed but version number did not:");
                print!("{}", diff);
            },
            CrateStatus::NotPublished => println!("🟡 {crate_name:-padding$} Crate not found in crates.io (Possible cause: Not published yet?)"),
        }
    }
}

async fn check_crate_status(crate_name: String) -> CrateStatus {
    if !check_crate_published(&crate_name).await {
        return CrateStatus::NotPublished;
    }
    download_crate(&crate_name, "./crates_io").await;

    let manifests = Manifests::read(&crate_name);
    let manifest_diff = manifests.diff();

    match (
        diff_crate_src(&crate_name, true),
        manifest_diff.is_some(),
        manifests.diff_versions(),
    ) {
        (_, _, true) => CrateStatus::VersionBumped,
        (false, false, _) => CrateStatus::Unchanged,
        (true, _, false) => CrateStatus::CodeChanged,
        (false, true, false) => CrateStatus::ManifestChanged(manifest_diff.unwrap()),
    }
}

fn read_publish_list() -> Vec<String> {
    #[derive(Deserialize)]
    struct PublishList {
        publish_list: Vec<String>,
    }

    let list_toml = fs::read_to_string(PUBLISH_LIST_PATH).unwrap();
    let list: PublishList = toml::from_str(&list_toml).unwrap();
    list.publish_list
}

/// Checks crates.io to see if the crate has been published. Returns `true` if it has.
async fn check_crate_published(crate_name: &str) -> bool {
    // We have to identify ourselves, or crates.io will ignore our request
    // TODO: Add an email to the user_agent filed - this will make us less likely to have our
    // access blocked.
    let client = reqwest::Client::builder()
        .user_agent("fluvio-check-crate-version")
        .build()
        .unwrap();
    let url = format!("https://crates.io/api/v1/crates/{crate_name}/versions");
    let res = client.get(url).send().await.unwrap();

    res.status() != 404
}

/// Returns `true` if the local crate source is different from crates.io
fn diff_crate_src(crate_name: &str, silent: bool) -> bool {
    let mut cmd = Command::new("diff");
    cmd.arg("-brq")
        .arg(format!("crates_io/{crate_name}/src"))
        .arg(format!("../../crates/{crate_name}/src"));
    if silent {
        !cmd.output().unwrap().status.success()
    } else {
        !cmd.status().unwrap().success()
    }
}

struct Manifests {
    local: toml::Value,
    crates_io: toml::Value,
}

impl Manifests {
    fn read(crate_name: &str) -> Self {
        let local_path = PathBuf::from(CRATES_DIR)
            .join(crate_name)
            .join("Cargo.toml");
        let crates_io_path = PathBuf::from(CRATES_IO_DIR)
            .join(crate_name)
            .join("Cargo.toml.orig");

        let local_text = fs::read_to_string(&local_path).unwrap();
        let crates_io_text = fs::read_to_string(&crates_io_path).unwrap();

        let local = toml::from_str::<toml::Value>(&local_text).unwrap();
        let crates_io = toml::from_str::<toml::Value>(&crates_io_text).unwrap();

        Self { local, crates_io }
    }

    fn diff_versions(&self) -> bool {
        self.local["package"]["version"] != self.crates_io["package"]["version"]
    }

    fn diff(&self) -> Option<String> {
        let diff = TomlDiff::diff(&self.local, &self.crates_io);
        if diff.changes.is_empty() {
            None
        } else {
            Some(diff.to_string())
        }
    }
}
