use std::{cmp::max, collections::HashMap, fs, path::PathBuf, process::Command};

use clap::Parser;
use serde::Deserialize;
use toml_diff::TomlDiff;

mod download;

use download::download_crate;

enum CrateStatus {
    NotPublished,
    VersionBumped(String),
    Unchanged,
    CodeChanged(Option<String>),
    ManifestChanged(String),
}

#[derive(Parser, Debug)]
pub struct Cli {
    #[clap(short, long)]
    verbose: bool,
    #[clap(long, env, default_value = "./publish-list.toml")]
    publish_list_path: String,
    #[clap(long, env, default_value = "../../crates")]
    crates_dir: String,
    #[clap(long, env, default_value = "./crates_io")]
    crates_io_dir: String,
}

#[tokio::main]
async fn main() {
    let cli = Cli::parse();
    let verbose = cli.verbose;

    let publish_list = read_publish_list(&cli.publish_list_path);
    let padding = publish_list
        .iter()
        .fold(0, |len, name| max(len, name.len()));

    // Start downloading crates, one per second, to comply with crates.io's crawler policy
    // https://crates.io/policies#crawlers
    let mut status_checks = Vec::with_capacity(publish_list.len());
    let mut publish_list = publish_list.into_iter();
    if let Some(crate_name) = publish_list.next() {
        status_checks.push((
            crate_name.clone(),
            tokio::spawn(check_crate_status(
                crate_name,
                cli.crates_dir.clone(),
                cli.crates_io_dir.clone(),
            )),
        ));
    }
    for crate_name in publish_list {
        std::thread::sleep(std::time::Duration::from_secs(1));
        status_checks.push((
            crate_name.clone(),
            tokio::spawn(check_crate_status(
                crate_name,
                cli.crates_dir.clone(),
                cli.crates_io_dir.clone(),
            )),
        ));
    }

    let mut crate_status: HashMap<String, CrateStatus> =
        HashMap::with_capacity(status_checks.len());

    for status_check in status_checks {
        let (crate_name, status) = status_check;
        let status = status.await.unwrap();
        crate_status.insert(crate_name, status);
    }
    let mut version_needs_bump = false;
    for (crate_name, status) in crate_status.into_iter() {
        match status {
            CrateStatus::VersionBumped(manifest_diff) => {
                println!("🟢 {crate_name:-padding$} Version number has been updated");
                diff_crate_src(&crate_name, &cli.crates_dir, &cli.crates_io_dir, verbose);
                if verbose {
                    println!("{manifest_diff}");
                }
            }
            CrateStatus::Unchanged => println!("🟢 {crate_name:-padding$} Code does not differ from crates.io"),
            CrateStatus::CodeChanged(manifest_diff) => {
                println!("⛔ {crate_name:-padding$} Code changed but version number did not:");
                diff_crate_src(&crate_name, &cli.crates_dir, &cli.crates_io_dir, true);
                if let Some(manifest_diff) = manifest_diff {
                    println!("{manifest_diff}");
                }
                version_needs_bump = true;
            },
            CrateStatus::ManifestChanged(manifest_diff) => {
                println!("⛔ {crate_name:-padding$} Manifest (Cargo.toml) changed but version number did not:");
                print!("{manifest_diff}");
                version_needs_bump = true;
            },
            CrateStatus::NotPublished => println!("🟡 {crate_name:-padding$} Crate not found in crates.io (Possible cause: Not published yet?)"),
        }
    }
    if version_needs_bump {
        panic!("Some of the crates need version bump");
    }
}

async fn check_crate_status(
    crate_name: String,
    crates_dir: String,
    crates_io_dir: String,
) -> CrateStatus {
    if !check_crate_published(&crate_name).await {
        return CrateStatus::NotPublished;
    }
    download_crate(&crate_name, &crates_io_dir).await;

    let manifests = Manifests::read(&crate_name, crates_dir.as_ref(), crates_io_dir.as_ref());
    let manifest_diff = manifests.diff();

    match (
        diff_crate_src(&crate_name, &crates_dir, &crates_io_dir, false),
        manifest_diff.is_some(),
        manifests.diff_versions(),
    ) {
        (_, _, true) => CrateStatus::VersionBumped(manifest_diff.unwrap()),
        (false, false, _) => CrateStatus::Unchanged,
        (true, _, false) => CrateStatus::CodeChanged(manifest_diff),
        (false, true, false) => CrateStatus::ManifestChanged(manifest_diff.unwrap()),
    }
}

fn read_publish_list(path: &str) -> Vec<String> {
    #[derive(Deserialize)]
    struct PublishList {
        publish_list: Vec<String>,
    }

    let list_toml = fs::read_to_string(path).unwrap_or_else(|_| panic!("{path} not found"));
    let list: PublishList = toml::from_str(&list_toml).unwrap();
    list.publish_list
}

/// Checks crates.io to see if the crate has been published. Returns `true` if it has.
async fn check_crate_published(crate_name: &str) -> bool {
    // We have to identify ourselves, or crates.io will ignore our request
    let client = reqwest::Client::builder()
        .user_agent("fluvio-check-crate-version (team@infinyon.com)")
        .build()
        .unwrap();
    let url = format!("https://crates.io/api/v1/crates/{crate_name}/versions");
    let res = client.get(url).send().await.unwrap();

    res.status() != 404
}

/// Returns `true` if the local crate source is different from crates.io
fn diff_crate_src(crate_name: &str, crates_dir: &str, crates_io_dir: &str, verbose: bool) -> bool {
    let mut cmd = Command::new("diff");
    cmd.arg("-brq")
        .arg(format!("{crates_io_dir}/{crate_name}/src"))
        .arg(format!("{crates_dir}/{crate_name}/src"));
    if verbose {
        !cmd.status().unwrap().success()
    } else {
        !cmd.output().unwrap().status.success()
    }
}

struct Manifests {
    local: toml::Value,
    crates_io: toml::Value,
}

impl Manifests {
    fn read(crate_name: &str, crates_dir: &str, crates_io_dir: &str) -> Self {
        let local_path = PathBuf::from(crates_dir)
            .join(crate_name)
            .join("Cargo.toml");
        let crates_io_path = PathBuf::from(crates_io_dir)
            .join(crate_name)
            .join("Cargo.toml.orig");

        let local_text = fs::read_to_string(&local_path)
            .unwrap_or_else(|_| panic!("{} not found", local_path.display()));
        let crates_io_text = fs::read_to_string(crates_io_path).unwrap();

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
