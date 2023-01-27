use std::fs;
use std::path::PathBuf;

use flate2::read::GzDecoder;
use semver::Version;
use serde_json::Value as JsonValue;
use tar::Archive;

const API_URL: &str = "https://crates.io/api/v1/crates";

async fn get_latest_version(crate_name: &str) -> Version {
    let url = format!("{API_URL}/{crate_name}/versions");
    let client = reqwest::Client::builder()
        .user_agent("fluvio-check-crate-version")
        .build()
        .unwrap();
    let res: JsonValue = client.get(&url).send().await.unwrap().json().await.unwrap();

    // Parse response as a list of `semver::Version`s
    let versions = res
        .pointer("/versions")
        .and_then(|vs| vs.as_array())
        .map(|vs| {
            vs.iter()
                .filter_map(|v| {
                    v.as_object()
                        .and_then(|v| v.get("num"))
                        .and_then(|n| n.as_str())
                })
                .filter_map(|v| Version::parse(v).ok())
                .collect::<Vec<_>>()
        })
        .ok_or_else(|| format!("malformed response from {url}"))
        .unwrap();

    versions
        .into_iter()
        .max()
        .expect("No versions found for crate {crate_name} on crates.io")
}

async fn download_crate_archive(name: &str, version: &Version) -> Vec<u8> {
    let url = format!("{API_URL}/{name}/{version}/download");
    println!("Downloading crate {name} = \"{version}\"");
    let client = reqwest::Client::builder()
        .user_agent("fluvio-check-crate-version")
        .build()
        .unwrap();
    client
        .get(&url)
        .send()
        .await
        .unwrap()
        .bytes()
        .await
        .unwrap()
        .into()
}

fn extract_crate(buf: Vec<u8>, name: &str, version: &Version, path: &str) {
    let extract_dir = PathBuf::from(path);
    fs::create_dir_all(&extract_dir).unwrap();

    // The path that the crate will be extracted to
    let extracted_path = extract_dir.join(format!("{name}-{version}"));
    if extracted_path.exists() {
        fs::remove_dir_all(&extracted_path).unwrap();
    }
    // The path that the crate will be renamed to after extraction
    let renamed_path = extract_dir.join(name);
    if renamed_path.exists() {
        fs::remove_dir_all(&renamed_path).unwrap();
    }
    println!("Extracting crate {name}");
    let gz_decoder = GzDecoder::new(&buf[..]);
    let mut archive = Archive::new(gz_decoder);
    archive
        .unpack(&extract_dir)
        .expect("Failed to extract crate {name}");
    // Remove the version from the crate name
    fs::rename(extracted_path, renamed_path).unwrap();
}

pub async fn download_crate(name: &str, path: &str) {
    let version = get_latest_version(name).await;
    let archive_data = download_crate_archive(name, &version).await;
    extract_crate(archive_data, name, &version, path);
}
