use std::io::Error as IoError;
use std::io::ErrorKind;
use std::str::FromStr;
use std::net::{IpAddr};
use std::process::Command;

use semver::Version;
use k8_client::K8Config;
use k8_config::KubeContext;
use url::{Url};
use structopt::StructOpt;
use colored::*;
use serde_json::{Value};
use fluvio_cluster::{ClusterInstaller, check_load_balancer_status};

use crate::CliError;
use super::*;

// constants
const MIN_KUBE_VERSION: &str = "1.7.0";
const DEFAULT_HELM_VERSION: &str = "3.2.0";
const SYS_CHART_VERSION: &str = "0.2.0";
const RESOURCE_SERVICE: &str = "service";
const RESOURCE_CRD: &str = "customresourcedefinitions";
const RESOURCE_SERVICE_ACCOUNT: &str = "secret";

#[derive(Debug, StructOpt)]
pub struct CheckCommand {
    /// run pre-install checks
    #[structopt(long)]
    pre_install: bool,
}

use async_trait::async_trait;
#[async_trait]
trait InstallCheck: Send + Sync + 'static {
    /// perform check, if successful return success message, if fail, return fail message
    async fn perform_check(&self) -> Result<String, String>;
}

struct LoadableConfig;

#[async_trait]
impl InstallCheck for LoadableConfig {
    async fn perform_check(&self) -> Result<String, String> {
        match check_loadable_config() {
            Ok(_) => Ok(
                "Kubernetes config is loadable and cluster hostname is not an IP address"
                    .to_string(),
            ),
            Err(err) => Err(format!(
                "Kubernetes cluster not found\n        error: {}",
                err.to_string()
            )),
        }
    }
}

struct K8Version;

#[async_trait]
impl InstallCheck for K8Version {
    async fn perform_check(&self) -> Result<String, String> {
        match check_version() {
            Ok(_) => Ok("Supported kubernetes version is installed".to_string()),
            Err(err) => Err(format!(
                "Supported kubernetes version v{} is not installed\n        error: {}",
                MIN_KUBE_VERSION,
                err.to_string()
            )),
        }
    }
}

struct HelmVersion;

#[async_trait]
impl InstallCheck for HelmVersion {
    async fn perform_check(&self) -> Result<String, String> {
        match check_helm_version() {
            Ok(_) => Ok("Supported helm version is installed".to_string()),
            Err(err) => Err(format!(
                "Supported helm version is not installed, > v{} is required\n        error: {}",
                DEFAULT_HELM_VERSION,
                err.to_string()
            )),
        }
    }
}

struct SysChart;

#[async_trait]
impl InstallCheck for SysChart {
    async fn perform_check(&self) -> Result<String, String> {
        match check_sys_charts() {
            Ok(_) => Ok("Fluvio system charts are installed".to_string()),
            Err(err) => Err(format!(
                "Compatible fluvio system charts are not installed, v{} is required\n        error: {}",
                SYS_CHART_VERSION,
                err.to_string()
            )),
        }
    }
}

struct CreateServicePermission;

#[async_trait]
impl InstallCheck for CreateServicePermission {
    async fn perform_check(&self) -> Result<String, String> {
        match check_permission(RESOURCE_SERVICE) {
            Ok(_) => Ok("Can create a Service".to_string()),
            Err(err) => Err(format!(
                "Cannot create a Service\n        error: {}",
                err.to_string()
            )),
        }
    }
}

struct CreateCrdPermission;

#[async_trait]
impl InstallCheck for CreateCrdPermission {
    async fn perform_check(&self) -> Result<String, String> {
        match check_permission(RESOURCE_CRD) {
            Ok(_) => Ok("Can create CustomResourceDefinitions".to_string()),
            Err(err) => Err(format!(
                "Cannot create CustomResourceDefinitions\n        error: {}",
                err.to_string()
            )),
        }
    }
}

struct CreateServiceAccountPermission;

#[async_trait]
impl InstallCheck for CreateServiceAccountPermission {
    async fn perform_check(&self) -> Result<String, String> {
        match check_permission(RESOURCE_SERVICE_ACCOUNT) {
            Ok(_) => Ok("Can create CustomResourceDefinitions".to_string()),
            Err(err) => Err(format!(
                "Cannot create CustomResourceDefinitions\n        error: {}",
                err.to_string()
            )),
        }
    }
}

struct LoadBalancer;

#[async_trait]
impl InstallCheck for LoadBalancer {
    async fn perform_check(&self) -> Result<String, String> {
        match check_load_balancer_status().await {
            Ok(_) => Ok("Load Balancer is up".to_string()),
            Err(err) => Err(format!(
                "Load Balancer is down\n        error: {}",
                err.to_string()
            )),
        }
    }
}

pub async fn run_checks(opt: CheckCommand) -> Result<String, CliError> {
    if opt.pre_install {
        run_preinstall_checks().await?;
    }
    Ok("".to_string())
}

async fn run_preinstall_checks() -> Result<(), CliError> {
    // List of checks
    let checks: Vec<Box<dyn InstallCheck>> = vec![
        Box::new(LoadableConfig),
        Box::new(K8Version),
        Box::new(HelmVersion),
        Box::new(SysChart),
        Box::new(CreateServicePermission),
        Box::new(CreateCrdPermission),
        Box::new(CreateServiceAccountPermission),
        Box::new(LoadBalancer),
    ];

    // capture failures if any
    let mut failures = Vec::new();
    println!("\nRunning pre-install checks....\n");

    for check in checks {
        match check.perform_check().await {
            Ok(success) => {
                let msg = format!("ok: {}", success);
                println!("✔️  {}", msg.green());
            }
            Err(failure) => {
                let msg = format!("failed: {}", failure);
                println!("❌ {}", msg.red());
                failures.push(failure);
            }
        }
    }

    // check if there are any failures and show final message
    if !failures.is_empty() {
        println!("\nSome pre-install checks have failed.\n");
        return Err(CliError::Other(
            "Some pre-install checks have failed.".to_string(),
        ));
    } else {
        println!("\nAll checks passed!\n");
    }
    Ok(())
}

fn check_permission(resource: &str) -> Result<(), IoError> {
    match check_create_permission(resource) {
        Ok(res) => {
            if !res {
                return Err(IoError::new(
                    ErrorKind::Other,
                    format!("create not permitted: {}", resource),
                ));
            }
        }
        Err(err) => {
            return Err(IoError::new(
                ErrorKind::Other,
                format!("Could not run permission check: {}", err.to_string()),
            ))
        }
    }
    Ok(())
}
fn get_current_context() -> Result<KubeContext, IoError> {
    let k8_config = K8Config::load().map_err(|err| {
        IoError::new(
            ErrorKind::Other,
            format!("unable to load kube context {}", err),
        )
    })?;
    match k8_config {
        K8Config::Pod(_) => Err(IoError::new(
            ErrorKind::Other,
            "Pod config is not valid here",
        )),
        K8Config::KubeConfig(config) => Ok(config),
    }
}

fn get_cluster_server_host() -> Result<String, IoError> {
    let kc_config = get_current_context()?;

    if let Some(ctx) = kc_config.config.current_cluster() {
        let server_url = ctx.cluster.server.to_owned();
        let url = match Url::parse(&server_url) {
            Ok(url) => url,
            Err(e) => {
                return Err(IoError::new(
                    ErrorKind::Other,
                    format!("error parsing server url {}", e.to_string()),
                ))
            }
        };
        if let Some(host) = url.host() {
            Ok(host.to_string())
        } else {
            Err(IoError::new(
                ErrorKind::Other,
                format!("no host found: {}", url),
            ))
        }
    } else {
        Err(IoError::new(ErrorKind::Other, "no context found"))
    }
}

fn check_loadable_config() -> Result<(), IoError> {
    let server_host = match get_cluster_server_host() {
        Ok(server) => server,
        Err(e) => {
            return Err(IoError::new(
                ErrorKind::Other,
                format!("error fetching server from kube context {}", e.to_string()),
            ))
        }
    };

    if !server_host.trim().is_empty() {
        if IpAddr::from_str(&server_host).is_ok() {
            return Err(IoError::new(ErrorKind::Other,
                 format!("Cluster in kube context cannot use IP address, please use minikube context: {}", server_host),
            ));
        };
    } else {
        return Err(IoError::new(
            ErrorKind::Other,
            "Cluster in kubectl context cannot have empty hostname".to_owned(),
        ));
    }

    Ok(())
}

fn check_version() -> Result<(), IoError> {
    let kube_version = Command::new("kubectl")
        .arg("version")
        .arg("-o=json")
        .output()
        .map_err(|err| {
            IoError::new(
                ErrorKind::Other,
                format!("Kubectl not found: {}", err.to_string()),
            )
        })?;
    let version_text = String::from_utf8(kube_version.stdout).unwrap();
    let kube_version_json: Value = serde_json::from_str(&version_text)?;
    let mut server_version = kube_version_json["serverVersion"]["gitVersion"].to_string();
    server_version.retain(|c| c != '"');
    let version_text_trimmed = &server_version[1..].trim();

    if Version::parse(&version_text_trimmed) < Version::parse(MIN_KUBE_VERSION) {
        return Err(IoError::new(
            ErrorKind::Other,
            format!(
            "Kubectl version {} is not compatible with fluvio platform, please install version >= {}",
            version_text_trimmed, MIN_KUBE_VERSION
        ),
        ));
    }
    Ok(())
}

fn check_helm_version() -> Result<(), IoError> {
    let helm_version = Command::new("helm")
        .arg("version")
        .arg("--short")
        .output()
        .map_err(|err| {
            IoError::new(
                ErrorKind::Other,
                format!("Helm package manager not found: {}", err.to_string()),
            )
        })?;
    let version_text = String::from_utf8(helm_version.stdout).unwrap();
    let version_text_trimmed = &version_text[1..].trim();

    if Version::parse(&version_text_trimmed) < Version::parse(DEFAULT_HELM_VERSION) {
        return Err(IoError::new(
            ErrorKind::Other,
            format!(
            "Helm version {} is not compatible with fluvio platform, please install version >= {}",
            version_text_trimmed, DEFAULT_HELM_VERSION
        ),
        ));
    }

    Ok(())
}

fn check_sys_charts() -> Result<(), IoError> {
    let sys_charts = ClusterInstaller::sys_charts().map_err(|err| {
        IoError::new(
            ErrorKind::Other,
            format!(
                "Error fetching installed fluvio system charts: {}",
                err.to_string()
            ),
        )
    })?;

    if sys_charts.is_empty() {
        return Err(IoError::new(
            ErrorKind::Other,
            "Fluvio system chart not found, please install fluvio-sys first".to_string(),
        ));
    } else if sys_charts.len() > 1 {
        return Err(IoError::new(
            ErrorKind::Other,
            "Multiple fluvio system charts found".to_string(),
        ));
    }
    Ok(())
}
