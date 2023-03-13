use std::path::Path;

use serde::{Deserialize, Serialize};
use surf::http::mime;
use surf::StatusCode;
use tracing::{debug, info};

use fluvio_hub_protocol::{PackageMeta, Result, HubError};
use fluvio_hub_protocol::constants::HUB_PACKAGE_EXT;

use crate::HubAccess;
use crate::{HUB_API_SM, HUB_API_CONN_PKG};
use crate::{package_get_meta, packagename_validate};

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
    let actiontoken = access.get_download_token().await?;
    get_package_with_token(pkgurl, &actiontoken).await
}

pub async fn get_package_with_token(pkgurl: &str, actiontoken: &str) -> Result<Vec<u8>> {
    let mut resp = surf::get(pkgurl)
        .header("Authorization", actiontoken)
        .await
        .map_err(|_| HubError::PackageDownload("authorization error".into()))?;

    match resp.status() {
        StatusCode::Ok => {}
        code => {
            let body_err_message = resp
                .body_string()
                .await
                .unwrap_or_else(|_err| "couldn't fetch error message".to_string());
            let msg = format!("Status({code}) {body_err_message}");
            return Err(HubError::PackageDownload(msg));
        }
    }

    // todo: validate package signing by owner
    // todo: validate package signing by hub

    let data = resp
        .body_bytes()
        .await
        .map_err(|_| HubError::PackageDownload("Data unpack failure".into()))?;
    Ok(data)
}

// deprecated, but keep for reference for a bit
pub async fn get_package_noauth(pkgurl: &str) -> Result<Vec<u8>> {
    //todo use auth
    let mut resp = surf::get(pkgurl)
        .await
        .map_err(|_| HubError::PackageDownload("".into()))?;
    match resp.status() {
        StatusCode::Ok => {}
        _ => {
            return Err(HubError::PackageDownload("".into()));
        }
    }
    let data = resp
        .body_bytes()
        .await
        .map_err(|_| HubError::PackageDownload("Data unpack failure".into()))?;
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
    push_package_api(&url, pkgpath, access).await
}

async fn push_package_api(put_url: &str, pkgpath: &str, access: &HubAccess) -> Result<()> {
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
            "{pkgfile} invalid name"
        )));
    }

    let pkg_bytes = std::fs::read(pkgpath)?;
    let actiontoken = access.get_publish_token().await?;
    let req = surf::put(put_url)
        .content_type(mime::BYTE_STREAM)
        .body_bytes(pkg_bytes)
        .header("Authorization", &actiontoken);
    let mut res = req
        .await
        .map_err(|e| HubError::HubAccess(format!("Failed to connect {e}")))?;

    match res.status() {
        surf::http::StatusCode::Ok => {
            println!("Package uploaded!");
            Ok(())
        }
        surf::http::StatusCode::Unauthorized => {
            Err(HubError::HubAccess("Unauthorized, please log in".into()))
        }
        _ => {
            debug!("push result: {} \n{res:?}", res.status());
            let bodymsg = res
                .body_string()
                .await
                .map_err(|_e| HubError::HubAccess("Failed to download err body".into()))?;
            let msg = format!("error status code({}) {}", res.status(), bodymsg);
            Err(HubError::HubAccess(msg))
        }
    }
}

#[cfg(test)]
mod util_tests {
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
}
