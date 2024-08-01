use std::fs::File;
use std::path::{Path, PathBuf};
use std::io::copy;

use http::StatusCode;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use tracing::{debug, info};

use fluvio_hub_protocol::{PackageMeta, Result, HubError};
use fluvio_hub_protocol::constants::HUB_PACKAGE_EXT;

use crate::htclient;
use crate::HubAccess;
use crate::{HUB_API_SM, HUB_API_CONN_PKG};
use crate::{package_get_meta, packagename_validate};
use crate::htclient::ResponseExt;

/// Used by hub server web api and cli exchange package lists
#[derive(Serialize, Deserialize)]
pub struct PackageList {
    pub packages: Vec<String>,
}

/// Used by hub server web api and cli exchange package lists
#[derive(Serialize, Deserialize)]
pub struct PackageListMeta {
    pub packages: Vec<PackageMeta>,
}

// returns (org, pname, ver)
pub fn cli_pkgname_split(pkgname: &str) -> Result<(&str, &str, &str)> {
    let idx1 = pkgname
        .rfind('@')
        .ok_or_else(|| HubError::InvalidPackageName(format!("{pkgname} missing version")))?;
    let split1 = pkgname.split_at(idx1); // this gives us (pkgname, ver)
    let (orgpkg, verstr) = split1;
    let ver = verstr.trim_start_matches('@');

    let idx2 = orgpkg.find('/').unwrap_or(0);
    let (org, pkgstr) = orgpkg.split_at(idx2);
    let pkg = pkgstr.trim_start_matches('/');

    Ok((org, pkg, ver))
}

/// Returns url string on sucess or Err(InvalidPackageName)
pub fn cli_pkgname_to_url(pkgname: &str, remote: &str) -> Result<String> {
    let (org, pkg, ver) = cli_pkgname_split(pkgname)?;
    // buildup something like: https://hub.infinyon.cloud/pkg/v0/sm/example/0.0.1
    let urlstring = if org.is_empty() {
        format!("{remote}/{HUB_API_SM}/{pkg}/{ver}")
    } else {
        format!("{remote}/{HUB_API_SM}/{org}/{pkg}/{ver}")
    };
    Ok(urlstring)
}

/// Returns url string on sucess or Err(InvalidPackageName)
pub fn cli_conn_pkgname_to_url(pkgname: &str, remote: &str, target: &str) -> Result<String> {
    let (org, pkg, ver) = cli_pkgname_split(pkgname)?;
    // buildup something like: https://hub.infinyon.cloud/connector/pkg/v0/sm/example/0.0.1
    let urlstring = if org.is_empty() {
        format!("{remote}/{HUB_API_CONN_PKG}/{target}/{pkg}/{ver}")
    } else {
        format!("{remote}/{HUB_API_CONN_PKG}/{target}/{org}/{pkg}/{ver}")
    };
    Ok(urlstring)
}

/// Returns filename on sucess or Err(InvalidPackageName)
pub fn cli_pkgname_to_filename(pkgname: &str) -> Result<String> {
    let (org, pkg, ver) = cli_pkgname_split(pkgname)?;
    // buildup something like: https://hub.infinyon.cloud/pkg/v0/sm/example/0.0.1
    let urlstring = if org.is_empty() {
        format!("{pkg}-{ver}.{HUB_PACKAGE_EXT}")
    } else {
        format!("{org}-{pkg}-{ver}.{HUB_PACKAGE_EXT}")
    };
    Ok(urlstring)
}

/// provide auth to get package from hub, save to path, validate on download
/// used by the cluster and the hub cli local download
/// returns recommended name and data
pub async fn get_package(pkgurl: &str, access: &HubAccess) -> Result<Vec<u8>> {
    let actiontoken = access.get_download_token().await.map_err(|err| {
        tracing::debug!(?err, "action error");
        err
    })?;
    tracing::trace!(tok = actiontoken, "have token");
    get_package_with_token(pkgurl, &actiontoken).await
}

pub async fn get_package_with_token(pkgurl: &str, actiontoken: &str) -> Result<Vec<u8>> {
    let req = http::Request::get(pkgurl)
        .header("Authorization", actiontoken)
        .body("")
        .map_err(|_| HubError::PackageDownload("request create error".into()))?;

    let resp = htclient::send(req)
        .await
        .map_err(|e| HubError::HubAccess(format!("Failed to connect {e}")))?;

    match resp.status() {
        StatusCode::OK => {}
        code => {
            let body_err_message = resp
                .body_string()
                .unwrap_or_else(|_err| "couldn't fetch error message".to_string());
            let msg = format!("Status({code}) {body_err_message}");
            return Err(HubError::PackageDownload(msg));
        }
    }

    // todo: validate package signing by owner
    // todo: validate package signing by hub

    let data = resp.body().to_owned();
    Ok(data)
}

// deprecated, but keep for reference for a bit
pub async fn get_package_noauth(pkgurl: &str) -> Result<Vec<u8>> {
    let resp = htclient::get(pkgurl)
        .await
        .map_err(|_| HubError::PackageDownload("".into()))?;

    let status = http::StatusCode::from_u16(resp.status().as_u16())
        .map_err(|e| HubError::General(format!("status mapping error {e}")))?;
    match status {
        StatusCode::OK => {}
        _ => {
            return Err(HubError::PackageDownload("".into()));
        }
    }
    let data = resp.body().to_vec();
    Ok(data)
}

/// non validating function to make canonical filenames from
/// org pkg version triples
pub fn make_filename(org: &str, pkg: &str, ver: &str) -> String {
    if org.is_empty() {
        format!("{pkg}-{ver}.{HUB_PACKAGE_EXT}")
    } else {
        format!("{org}-{pkg}-{ver}.{HUB_PACKAGE_EXT}")
    }
}

/// legacy name for pushing smartmodule package
pub async fn push_package(pkgpath: &str, access: &HubAccess) -> Result<()> {
    let pm = package_get_meta(pkgpath)?;
    let host = &access.remote;
    let url = format!(
        "{host}/{HUB_API_SM}/{}/{}/{}",
        pm.group, pm.name, pm.version
    );
    push_package_api(&url, pkgpath, access).await
}

/// push package to connector api
pub async fn push_package_conn(pkgpath: &str, access: &HubAccess, target: &str) -> Result<()> {
    info!("package target: {target}");
    let pm = package_get_meta(pkgpath)?;
    let host = &access.remote;
    let url = format!(
        "{host}/{HUB_API_CONN_PKG}/{target}/{}/{}/{}",
        pm.group, pm.name, pm.version
    );
    debug!(url, "package url");
    push_package_api(&url, pkgpath, access).await
}

/// push package to connector api
pub async fn push_package_sdf(pkgpath: &str, access: &HubAccess) -> Result<()> {
    use crate::{
        SDF_PKG_KIND, SDF_PKG_KIND_DATAFLOW, SDF_PKG_KIND_PACKAGE, HUB_API_SDF_DATAFLOW_PUB,
        HUB_API_SDF_PKG_PUB,
    };

    info!("sdf package push form: {pkgpath}");
    let pm = package_get_meta(pkgpath)?;
    let Some(sdf_kind) = pm.tag_get(SDF_PKG_KIND) else {
        let msg = format!("Invalid sdf hub package_meta: missing tag for {SDF_PKG_KIND}");
        return Err(HubError::PackagePublish(msg));
    };
    let sdf_kind = sdf_kind.first().cloned().unwrap_or_default().value;
    let endpoint = match sdf_kind.as_str() {
        SDF_PKG_KIND_DATAFLOW => HUB_API_SDF_DATAFLOW_PUB,
        SDF_PKG_KIND_PACKAGE => HUB_API_SDF_PKG_PUB,
        _ => {
            let msg = format!("Invalid sdf hub package_meta {SDF_PKG_KIND}: {sdf_kind}");
            return Err(HubError::PackagePublish(msg));
        }
    };
    let host = &access.remote;
    let url = format!("{host}/{endpoint}/{}/{}/{}", pm.group, pm.name, pm.version);
    debug!(url, "package url");
    push_package_api(&url, pkgpath, access).await
}

pub async fn push_package_api(put_url: &str, pkgpath: &str, access: &HubAccess) -> Result<()> {
    let pm = package_get_meta(pkgpath)?;
    packagename_validate(&pm.name)?;

    // check that given pkg file matches name
    let pkgfile = Path::new(pkgpath)
        .file_name()
        .unwrap()
        .to_string_lossy()
        .to_string();
    if pkgfile != pm.packagefile_name() {
        return Err(HubError::InvalidPackageName(format!(
            "{pkgfile} invalid name {}",
            pm.packagefile_name()
        )));
    }

    tracing::debug!("get action token");
    let pkg_bytes = std::fs::read(pkgpath)?;
    let actiontoken = access.get_publish_token().await?;

    tracing::debug!(url = put_url, "put package");
    let req = http::Request::put(put_url)
        .header("Authorization", &actiontoken)
        .header(http::header::CONTENT_TYPE, mime::OCTET_STREAM.as_str())
        .body(pkg_bytes)
        .map_err(|e| HubError::HubAccess(format!("request formatting error {e}")))?;
    debug!("auth accepted");
    let res = crate::htclient::send(req)
        .await
        .map_err(|e| HubError::HubAccess(format!("Failed to connect {e}")))?;
    debug!(res=?res, "auth accepted");
    let status = http::StatusCode::from_u16(res.status().as_u16())
        .map_err(|e| HubError::General(format!("unknown status code: {e}")))?;
    match status {
        StatusCode::OK => {
            println!("Package uploaded!");
            Ok(())
        }
        StatusCode::UNAUTHORIZED => Err(HubError::HubAccess("Unauthorized, please log in".into())),
        StatusCode::CONFLICT => Err(HubError::PackageAlreadyPublished(
            "Make sure version is up to date and name doesn't conflicts with other package.".into(),
        )),
        _ => {
            debug!("push result: {} \n{res:?}", res.status());
            let bodymsg = res
                .body_string()
                .map_err(|_e| HubError::HubAccess("Failed to download err body".into()))?;
            let msg = format!("error status code({}) {}", status, bodymsg);
            Err(HubError::HubAccess(msg))
        }
    }
}

/// Generates Sha256 checksum for a given file
pub fn sha256_digest(path: &PathBuf) -> Result<String> {
    let mut hasher = Sha256::new();
    let mut file = File::open(path)?;

    copy(&mut file, &mut hasher)?;

    let hash_bytes = hasher.finalize();

    Ok(hex::encode(hash_bytes))
}

#[cfg(test)]
mod util_tests {
    use tempfile::TempDir;

    use crate::sha256_digest;

    use super::cli_pkgname_split;
    use super::cli_pkgname_to_url;
    use super::cli_pkgname_to_filename;
    use super::cli_conn_pkgname_to_url;

    #[test]
    fn cli_pkgname_split_t() {
        let recs_good = vec![
            ("example@0.0.1", ("", "example", "0.0.1")),
            ("infinyon/example@0.0.1", ("infinyon", "example", "0.0.1")),
        ];
        for rec in recs_good {
            let out = cli_pkgname_split(rec.0);
            assert!(out.is_ok());
            let (org, pkg, ver) = out.unwrap();

            assert_eq!(rec.1 .0, org);
            assert_eq!(rec.1 .1, pkg);
            assert_eq!(rec.1 .2, ver);
        }
    }

    #[test]
    fn cli_pkgname_to_url_t() {
        let recs_good = vec![
            (
                "example@0.0.1",
                "https://hub.infinyon.cloud/hub/v0/pkg/pub/example/0.0.1",
            ),
            (
                "infinyon/example@0.0.1",
                "https://hub.infinyon.cloud/hub/v0/pkg/pub/infinyon/example/0.0.1",
            ),
        ];
        let remote = "https://hub.infinyon.cloud";
        for rec in recs_good {
            let out = cli_pkgname_to_url(rec.0, remote);
            assert!(out.is_ok());
            let url = out.unwrap();
            assert_eq!(rec.1, &url);
        }
    }

    #[test]
    fn cli_pkgname_to_filename_t() {
        let recs_good = vec![
            ("example@0.0.1", "example-0.0.1.ipkg"),
            ("infinyon/example@0.0.1", "infinyon-example-0.0.1.ipkg"),
        ];
        for rec in recs_good {
            let out = cli_pkgname_to_filename(rec.0);
            assert!(out.is_ok());
            let url = out.unwrap();
            assert_eq!(rec.1, &url);
        }
    }

    #[test]
    fn cli_conn_pkgname_to_url_t() {
        let recs_good = vec![
            (
                "example@0.0.1",
                "https://hub.infinyon.cloud/hub/v0/connector/pkg/aarch64-unknown-linux-musl/example/0.0.1",
            ),
            (
                "infinyon/example@0.0.1",
                "https://hub.infinyon.cloud/hub/v0/connector/pkg/aarch64-unknown-linux-musl/infinyon/example/0.0.1",
            ),
        ];
        let remote = "https://hub.infinyon.cloud";
        for rec in recs_good {
            let out = cli_conn_pkgname_to_url(rec.0, remote, "aarch64-unknown-linux-musl");
            assert!(out.is_ok());
            let url = out.unwrap();
            assert_eq!(rec.1, &url);
        }
    }

    #[test]
    fn creates_shasum_digest() {
        use std::fs::write;

        let tempdir = TempDir::new().unwrap().into_path().to_path_buf();
        let foo_path = tempdir.join("foo");

        write(&foo_path, "foo").unwrap();

        let foo_a_checksum = sha256_digest(&foo_path).unwrap();

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

        let foo_checksum = sha256_digest(&foo_path).unwrap();
        let bar_checksum = sha256_digest(&bar_path).unwrap();

        assert_ne!(foo_checksum, bar_checksum);
    }

    #[test]
    fn checks_files_checksums_same() {
        use std::fs::write;

        let tempdir = TempDir::new().unwrap().into_path().to_path_buf();
        let foo_path = tempdir.join("foo");

        write(&foo_path, "foo").unwrap();

        let foo_a_checksum = sha256_digest(&foo_path).unwrap();
        let foo_b_checksum = sha256_digest(&foo_path).unwrap();

        assert_eq!(foo_a_checksum, foo_b_checksum);
    }
}
