//! Download API for downloading the artifacts from the server

use std::path::PathBuf;
use std::io::{Cursor, copy};
use std::fs::File;

use anyhow::{Error, Result};
use async_trait::async_trait;
use http::StatusCode;
use tracing::instrument;

use crate::fvm::Artifact;
use crate::utils::sha256_digest;
use crate::htclient;

/// Verifies downloaded artifact checksums against the upstream checksums
async fn checksum(artf: &Artifact, path: &PathBuf) -> Result<()> {
    let local_file_shasum = sha256_digest(path)?;
    let body_shasum = htclient::get(&artf.sha256_url)
        .await
        .map_err(|err| Error::msg(err.to_string()))?
        .into_body();
    let upstream_shasum = String::from_utf8_lossy(&body_shasum);

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
    /// and returns the path to the downloaded artifact
    async fn download(&self, target_dir: PathBuf) -> Result<PathBuf>;
}

#[async_trait]
impl Download for Artifact {
    #[cfg(target_arch = "wasm32")]
    async fn download(&self, target_dir: PathBuf) -> Result<PathBuf> {
        unimplemented!("The download support is not implemented for wasm32 architecture");
    }

    #[cfg(not(target_arch = "wasm32"))]
    #[instrument(skip(self, target_dir))]
    async fn download(&self, target_dir: PathBuf) -> Result<PathBuf> {
        tracing::info!(
            name = self.name,
            download_url = ?self.download_url,
            "Downloading artifact"
        );

        let res = htclient::get(&self.download_url)
            .await
            .map_err(|err| Error::msg(err.to_string()))?;

        let status = http::StatusCode::from_u16(res.status().as_u16())?;
        if status == StatusCode::OK {
            let out_path = target_dir.join(&self.name);
            let mut file = File::create(&out_path)?;
            let bytes = res.into_body();
            let mut buf = Cursor::new(&bytes);

            copy(&mut buf, &mut file)?;
            checksum(self, &out_path).await?;

            tracing::debug!(
                name = self.name,
                out_path = ?out_path.display(),
                "Artifact downloaded",
            );

            return Ok(out_path);
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
        let download_path = artifact.download(target_dir.clone()).await.unwrap();

        assert!(target_dir.join("fluvio").exists());
        assert_eq!(download_path, target_dir.join("fluvio"));
    }

    #[ignore]
    #[fluvio_future::test]
    async fn downloaded_artifact_matches_upstream_checksum() {
        use htclient::ResponseExt;

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
        let upstream_shasum = htclient::get(&artifact.sha256_url)
            .await
            .map_err(|err| Error::msg(err.to_string()))
            .unwrap()
            .body_string()
            .map_err(|err| Error::msg(err.to_string()))
            .unwrap();

        assert_eq!(downstream_shasum, upstream_shasum);
    }
}
