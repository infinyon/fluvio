// TODO: Remove this
#![allow(unused)]

use std::{
    cmp::max,
    fs,
    path::{Path, PathBuf},
    process::{Command, Stdio},
};

use flate2::bufread::GzDecoder;
use tar::Archive;
use serde::Deserialize;
use walkdir::WalkDir;
use which::which_all;

const PUBLISH_LIST_PATH: &str = "./publish-list.toml";
const CRATES_DIR: &str = "../../crates";
const CRATES_IO_DIR: &str = "./crates_io";
const DB_SNAPHSOT_DIR: &str = "./crates_io/db_snapshot";
const DB_SNAPSHOT_URL: &str = "https://static.crates.io/db-dump.tar.gz";

fn main() {
    let publish_list = read_publish_list();
    // download_crates_io_data();
    check_install_cargo_download();

    download_all_crates(&publish_list);

    // Compute the padding based on the length of the longest crate name
    let padding = publish_list
        .iter()
        .fold(0, |len, name| max(len, name.len()));

    for crate_name in publish_list {
        let manifests = Manifests::read(&crate_name);

        manifests.diff();
        check_crate_published(&crate_name);

        match (diff_crate_src(&crate_name), manifests.diff(), manifests.diff_versions()) {
            (_, _, true) => println!("🟢 {crate_name:-padding$} Version number has been updated"),
            (false, false, _) => println!("🟢 {crate_name:-padding$} Code does not differ from crates.io"),
            (true, _, false) => println!("⛔ {crate_name:-padding$} Code changed but version number did not"),
            (false, true, false) => println!("⛔ {crate_name:-padding$} Manifest (Cargo.toml) changed but version number did not")
        }
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
fn check_crate_published(crate_name: &str) -> bool {
    // We have to identify ourselves, or crates.io will ignore our request
    // TODO: Add an email to the user_agent filed - this will make us less likely to have our
    // access blocked.
    let client = reqwest::blocking::Client::builder()
        .user_agent("fluvio-check-crate-version")
        .build()
        .unwrap();
    let url = "https://crates.io/api/v1/crates/{crate_name}/versions";
    let response = client
        .get(url)
        .send()
        // TODO: Don't unwrap this, check for {"errors": [{"detail": "Not Found"}]}, or maybe a 404 response
        .unwrap()
        .text()
        .unwrap();

    let json: serde_json::Value =
        serde_json::from_str(&response).expect("Should recieve a valid JSON response");
    match json.get("errors") {
        None => true,
        Some(errors) => {
            let errors = errors.as_array().expect("'errors' should be an array");
            let error = errors
                .get(0)
                .expect("'errors' array should contain at least one error");
            let detail = error
                .get("detail")
                .expect("error should have a key named 'detail'");
            let detail = detail
                .as_str()
                .expect("The value of 'detail' should be a string");
            if detail == "Not Found" {
                false
            } else {
                panic!("Error when retrieving info for {crate_name}.\n{url} returned response:\n{response}");
            }
        }
    }
}

fn download_all_crates(crate_list: &Vec<String>) {
    for crate_name in crate_list {
        download_crate(crate_name);
    }
}

fn download_crate(crate_name: &str) {
    let crate_path = PathBuf::from(CRATES_IO_DIR).join(crate_name);
    if crate_path.exists() {
        println!("Removing {}", crate_path.display());
        fs::remove_dir_all(&crate_path).unwrap();
    }
    fs::create_dir_all(&crate_path).unwrap();

    println!("Downloading crate {crate_name}");
    Command::new("cargo")
        .arg("download")
        .arg("-x")
        .arg(crate_name)
        .arg("-o")
        .arg(crate_path)
        // .arg("--verbose")
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .output()
        .expect(&format!("Failed to download crate {crate_name}"));
}

/// Install cargo-download if it is not already installed.
fn check_install_cargo_download() {
    // TODO: Find a way to do this without external commands
    if which_all("cargo-download").unwrap().next().is_none() {
        println!("cargo-download not found");
        println!("Installing cargo-download");
        Command::new("cargo")
            .arg("install")
            .arg("cargo-download")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .output()
            .expect("Failed to install cargo-download");
    } else {
        println!("🔧 cargo-download found");
    }
}

/// Returns `true` if the local crate source is different from crates.io
fn diff_crate_src(crate_name: &str) -> bool {
    let local_src_dir = PathBuf::from(CRATES_DIR).join(crate_name).join("src");
    let crates_io_src_dir = PathBuf::from(CRATES_IO_DIR).join(crate_name).join("src");

    let mut local_src_walker = WalkDir::new(local_src_dir)
        .sort_by(|a, b| a.file_name().cmp(b.file_name()))
        .into_iter();
    let mut crates_io_src_walker = WalkDir::new(crates_io_src_dir)
        .sort_by(|a, b| a.file_name().cmp(b.file_name()))
        .into_iter();

    for (local, crates_io) in (&mut local_src_walker).zip(&mut crates_io_src_walker) {
        let (local, crates_io) = (local.unwrap(), crates_io.unwrap());
        if local.depth() != crates_io.depth()
            || local.file_type() != crates_io.file_type()
            || local.file_name() != crates_io.file_name()
        {
            return true;
        } else {
            if local.file_type().is_file() {
                let local_content = fs::read(local.path()).unwrap();
                let crates_io_content = fs::read(crates_io.path()).unwrap();

                if local_content != crates_io_content {
                    return true;
                }
            }
        }
    }
    false
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
            .join("Cargo.toml");

        let local_text = fs::read_to_string(&local_path).unwrap();
        let crates_io_text = fs::read_to_string(&crates_io_path).unwrap();

        let local = toml::from_str::<toml::Value>(&local_text).unwrap();
        let crates_io = toml::from_str::<toml::Value>(&crates_io_text).unwrap();

        Self { local, crates_io }
    }

    fn diff_versions(&self) -> bool {
        self.local["package"]["version"] != self.crates_io["package"]["version"]
    }

    fn diff(&self) -> bool {
        // Recursively searches for differences in the manifests
        fn diff_rec(local_val: &toml::Value, crates_io_val: &toml::Value) -> bool {
            use toml::Value::*;
            match (local_val, crates_io_val) {
                (String(local_str), String(crates_io_str)) => local_str != crates_io_str,
                (Integer(local_int), Integer(crates_io_int)) => local_int != crates_io_int,
                (Float(local_float), Float(crates_io_float)) => local_float != crates_io_float,
                (Boolean(local_bool), Boolean(crates_io_bool)) => local_bool != crates_io_bool,
                (Datetime(local_dt), Datetime(crates_io_dt)) => local_dt != crates_io_dt,
                (Array(local_array), Array(crates_io_array)) => {
                    for (local_val, crates_io_val) in local_array.iter().zip(crates_io_array) {
                        if diff_rec(local_val, crates_io_val) {
                            return true;
                        }
                    }
                    false
                }
                (Table(local_table), Table(crates_io_table)) => {
                    // TODO: Make sure this can't get messed up if the keys come in different
                    // orders
                    for ((local_key, local_val), (crates_io_key, crates_io_val)) in
                        local_table.iter().zip(crates_io_table)
                    {
                        if local_key != crates_io_key || diff_rec(local_val, crates_io_val) {
                            return true;
                        }
                    }
                    false
                }
                _ => true,
            }
        }
        diff_rec(&self.local, &self.crates_io)
    }
}
