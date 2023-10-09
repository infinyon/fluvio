//! Download API for downloading the artifacts from the server

use std::path::PathBuf;
use std::io::{Cursor, copy};
use std::fs::File;

use anyhow::{Error, Result};
use async_trait::async_trait;
use reqwest::StatusCode;
use tracing::instrument;

use crate::fvm::Artifact;
use crate::utils::sha256_digest;

/// Verifies downloaded artifact checksums against the upstream checksums
async fn checksum(artf: &Artifact, path: &PathBuf) -> Result<()> {
    let local_file_shasum = sha256_digest(path)?;
    let upstream_shasum = reqwest::get(artf.sha256_url.as_str())
        .await
        .map_err(|err| Error::msg(err.to_string()))?
        .text()
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
    #[instrument(skip(self, target_dir))]
    async fn download(&self, target_dir: PathBuf) -> Result<()> {
        tracing::info!(
            name = self.name,
            download_url = ?self.download_url,
            "Downloading artifact"
        );

        let res = reqwest::get(self.download_url.as_str())
            .await
            .map_err(|err| Error::msg(err.to_string()))?;

        if res.status() == StatusCode::OK {
            let out_path = target_dir.join(&self.name);
            let mut file = File::create(&out_path)?;
            let mut buf = Cursor::new(
                res.bytes()
                    .await
                    .map_err(|err| Error::msg(err.to_string()))?,
            );

            copy(&mut buf, &mut file)?;
            checksum(self, &out_path).await?;

            tracing::debug!(
                name = self.name,
                out_path = ?out_path.display(),
                "Artifact downloaded",
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

    #[ignore]
    #[fluvio_future::test]
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

    #[ignore]
    #[fluvio_future::test]
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
        let downstream_shasum = sha256_digest(&binary_path).unwrap();
        let upstream_shasum = reqwest::get(artifact.sha256_url.as_str())
            .await
            .map_err(|err| Error::msg(err.to_string()))
            .unwrap()
            .text()
            .await
            .map_err(|err| Error::msg(err.to_string()))
            .unwrap();

        assert_eq!(downstream_shasum, upstream_shasum);
    }
}
