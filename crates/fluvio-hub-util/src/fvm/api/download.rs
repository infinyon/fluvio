//! Download API for downloading the artifacts from the server

use std::path::{PathBuf};
use std::io::{Cursor, copy, Read};
use std::fs::File;

use anyhow::{Error, Result};
use http_client::async_trait;
use sha2::{Sha256, Digest};
use surf::StatusCode;

use crate::fvm::Artifact;

/// Generates the Sha256 checksum for the specified file
fn shasum256(file: &File) -> Result<String> {
    let meta = file.metadata()?;
    let mut hasher = Sha256::new();
    let mut buffer = vec![0; meta.len() as usize];

    file.read_exact(&mut buffer)?;
    hasher.update(buffer);

    let output = hasher.finalize();
    Ok(hex::encode(output))
}

/// Verifies downloaded artifact checksums against the upstream checksums
async fn checksum(artf: &Artifact, path: &PathBuf) -> Result<()> {
    let file = File::open(path)?;
    let local_file_shasum = shasum256(&file)?;
    let upstream_shasum = surf::get(&artf.sha256_url)
        .await
        .map_err(|err| Error::msg(err.to_string()))?
        .body_string()
        .await
        .map_err(|err| Error::msg(err.to_string()))?;

    if local_file_shasum != upstream_shasum {
        return Err(Error::msg(format!(
            "Artifact {} didnt matched upstream shasum. {} != {}",
            artf.name, local_file_shasum, upstream_shasum
        )));
    }

    Ok(())
}

#[async_trait]
pub trait Download {
    /// Downloads the artifact to the specified directory
    ///
    /// Internally validates the checksum of the downloaded artifact
    async fn download(&self, target_dir: PathBuf) -> Result<()>;
}

#[async_trait]
impl Download for Artifact {
    async fn download(&self, target_dir: PathBuf) -> Result<()> {
        let mut res = surf::get(&self.download_url)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;

        if res.status() == StatusCode::Ok {
            let out_path = target_dir.join(&self.name);
            let mut file = File::create(&out_path)?;
            let mut buf = Cursor::new(
                res.body_bytes()
                    .await
                    .map_err(|err| Error::msg(err.to_string()))?,
            );

            copy(&mut buf, &mut file)?;
            checksum(self, &out_path).await?;

            tracing::debug!(
                "Artifact downloaded: {} at {:?}",
                self.name,
                out_path.display()
            );

            return Ok(());
        }

        Err(Error::msg(format!(
            "Server responded with Status Code {}",
            res.status()
        )))
    }
}

#[cfg(test)]
mod test {
    use tempfile::TempDir;

    use super::*;

    #[async_std::test]
    async fn download_artifact() {
        let target_dir = TempDir::new().unwrap().into_path().to_path_buf();
        let artifact = Artifact {
            name: "fluvio".to_string(),
            version: "0.10.15".parse().unwrap(),
            download_url: "https://packages.fluvio.io/v1/packages/fluvio/fluvio/0.10.15/aarch64-apple-darwin/fluvio".parse().unwrap(),
            sha256_url: "https://packages.fluvio.io/v1/packages/fluvio/fluvio/0.10.15/aarch64-apple-darwin/fluvio.sha256".parse().unwrap(),
        };

        artifact.download(target_dir.clone()).await.unwrap();
        assert!(target_dir.join("fluvio").exists());
    }

    #[async_std::test]
    async fn downloaded_artifact_matches_upstream_checksum() {
        let target_dir = TempDir::new().unwrap().into_path().to_path_buf();
        let artifact = Artifact {
            name: "fluvio".to_string(),
            version: "0.10.15".parse().unwrap(),
            download_url: "https://packages.fluvio.io/v1/packages/fluvio/fluvio/0.10.15/aarch64-apple-darwin/fluvio".parse().unwrap(),
            sha256_url: "https://packages.fluvio.io/v1/packages/fluvio/fluvio/0.10.15/aarch64-apple-darwin/fluvio.sha256".parse().unwrap(),
        };

        artifact.download(target_dir.clone()).await.unwrap();

        let binary_path = target_dir.join("fluvio");
        let file = File::open(binary_path).unwrap();
        let downstream_shasum = shasum256(&file).unwrap();
        let upstream_shasum = surf::get(&artifact.sha256_url)
            .await
            .map_err(|err| Error::msg(err.to_string()))
            .unwrap()
            .body_string()
            .await
            .map_err(|err| Error::msg(err.to_string()))
            .unwrap();

        assert_eq!(downstream_shasum, upstream_shasum);
    }

    #[test]
    fn creates_shasum_digest() {
        use std::fs::write;

        let tempdir = TempDir::new().unwrap().into_path().to_path_buf();
        let foo_path = tempdir.join("foo");

        write(&foo_path, "foo").unwrap();

        let foo_a_checksum = shasum256(&File::open(&foo_path).unwrap()).unwrap();

        assert_eq!(
            foo_a_checksum,
            "2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"
        );
    }

    #[test]
    fn checks_files_checksums_diff() {
        use std::fs::write;

        let tempdir = TempDir::new().unwrap().into_path().to_path_buf();
        let foo_path = tempdir.join("foo");
        let bar_path = tempdir.join("bar");

        write(&foo_path, "foo").unwrap();
        write(&bar_path, "bar").unwrap();

        let foo_checksum = shasum256(&File::open(&foo_path).unwrap()).unwrap();
        let bar_checksum = shasum256(&File::open(&bar_path).unwrap()).unwrap();

        assert_ne!(foo_checksum, bar_checksum);
    }

    #[test]
    fn checks_files_checksums_same() {
        use std::fs::write;

        let tempdir = TempDir::new().unwrap().into_path().to_path_buf();
        let foo_path = tempdir.join("foo");

        write(&foo_path, "foo").unwrap();

        let foo_a_checksum = shasum256(&File::open(&foo_path).unwrap()).unwrap();
        let foo_b_checksum = shasum256(&File::open(&foo_path).unwrap()).unwrap();

        assert_eq!(foo_a_checksum, foo_b_checksum);
    }
}
