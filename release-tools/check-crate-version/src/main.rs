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

    // download_all_crates(&publish_list);

    // Compute the padding based on the length of the longest crate name
    let padding = publish_list
        .iter()
        .fold(0, |len, name| max(len, name.len()));

    for crate_name in publish_list {
        if crate_src_changed(&crate_name) {
            println!("⛔ {crate_name:-padding$} Repo code has changed");
        } else {
            println!("🟢 {crate_name:-padding$} Repo code does not differ from crates.io");
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

fn download_crates_io_data() {
    fs::create_dir_all(&DB_SNAPHSOT_DIR).unwrap();

    println!("Downloading crates.io data: {DB_SNAPSHOT_URL}");
    // TODO: Find a way to not load the whole file into memory
    // TODO: Add progress indicator
    let archive_data = reqwest::blocking::get(DB_SNAPSHOT_URL)
        .unwrap()
        .bytes()
        .unwrap()
        .to_vec();

    println!("Decoding gzip and unpacking tar archive");
    // TODO: Add progress indicator
    let gz_decoder = GzDecoder::new(archive_data.as_ref());
    let mut archive = Archive::new(gz_decoder);
    archive.unpack(DB_SNAPHSOT_DIR).unwrap();
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

fn crate_src_changed(crate_name: &str) -> bool {
    let crate_src_dir = PathBuf::from(CRATES_DIR).join(crate_name).join("src");
    let crates_io_src_dir = PathBuf::from(CRATES_IO_DIR).join(crate_name).join("src");

    diff_dirs(&crate_src_dir, &crates_io_src_dir)
}

/// Returns `true` if the contents of dir `a` and `b` are different.
fn diff_dirs(a: &Path, b: &Path) -> bool {
    let mut a_it = WalkDir::new(a)
        .sort_by(|a, b| a.file_name().cmp(b.file_name()))
        .into_iter();
    let mut b_it = WalkDir::new(b)
        .sort_by(|a, b| a.file_name().cmp(b.file_name()))
        .into_iter();

    for (a, b) in (&mut a_it).zip(&mut b_it) {
        let (a, b) = (a.unwrap(), b.unwrap());
        if a.depth() != b.depth()
            || a.file_type() != b.file_type()
            || a.file_name() != b.file_name()
        {
            return true;
        } else {
            if a.file_type().is_file() && diff_files(&a.path(), &b.path()) {
                return true;
            }
        }
    }
    false
}

/// Returns `true` if the contents of `a` and `b` are different.
/// `a` and `b` must be files, or this will panic.
fn diff_files(a: &Path, b: &Path) -> bool {
    let a_contents = fs::read(a).unwrap();
    let b_contents = fs::read(b).unwrap();
    a_contents != b_contents
}
