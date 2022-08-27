use std::path::PathBuf;

use flate2::bufread::GzDecoder;
use tar::Archive;
use serde::Deserialize;

fn main() {
    let _publish_list = read_publish_list();
    download_crates_io_data();
}

fn read_publish_list() -> Vec<String> {
    #[derive(Deserialize)]
    struct PublishList {
        publish_list: Vec<String>,
    }

    let list_toml = std::fs::read_to_string("./publish-list.toml").unwrap();
    let list: PublishList = toml::from_str(&list_toml).unwrap();
    list.publish_list
}

fn download_crates_io_data() {
    let snapshot_dir = PathBuf::from("./crates_io/db_snapshot");
    let snapshot_url = "https://static.crates.io/db-dump.tar.gz";
    std::fs::create_dir_all(&snapshot_dir).unwrap();

    println!("Downloading crates.io data: {snapshot_url}");
    // TODO: Find a way to not load the whole file into memory
    // TODO: Add progress indicator
    let archive_data = reqwest::blocking::get(snapshot_url)
        .unwrap()
        .bytes()
        .unwrap()
        .to_vec();

    println!("Decoding gzip and unpacking tar archive");
    // TODO: Add progress indicator
    let gz_decoder = GzDecoder::new(archive_data.as_ref());
    let mut archive = Archive::new(gz_decoder);
    archive.unpack(snapshot_dir).unwrap();
}

