use flate2::bufread::GzDecoder;
use tar::Archive;
use serde::Deserialize;

const PUBLISH_LIST_PATH: &str = "./publish-list.toml";
const DB_SNAPHSOT_DIR: &str = "./crates_io/db_snapshot";
const DB_SNAPSHOT_URL: &str = "https://static.crates.io/db-dump.tar.gz";

fn main() {
    let _publish_list = read_publish_list();
    download_crates_io_data();
}

fn read_publish_list() -> Vec<String> {
    #[derive(Deserialize)]
    struct PublishList {
        publish_list: Vec<String>,
    }

    let list_toml = std::fs::read_to_string(PUBLISH_LIST_PATH).unwrap();
    let list: PublishList = toml::from_str(&list_toml).unwrap();
    list.publish_list
}

fn download_crates_io_data() {
    std::fs::create_dir_all(&DB_SNAPHSOT_DIR).unwrap();

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

