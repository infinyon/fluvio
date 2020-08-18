use std::io::Error as IoError;
use std::io::ErrorKind;
use std::str::FromStr;
use std::net::{IpAddr};
use std::process::Command;
use std::time::Duration;

use semver::Version;
use k8_client::K8Config;
use flv_future_aio::timer::sleep;
use k8_client::ClientError;
use k8_client::load_and_share;
use k8_obj_core::service::ServiceSpec;
use k8_obj_metadata::InputObjectMeta;
use k8_client::ClientError as K8ClientError;
use k8_config::KubeContext;
use url::{Url};
use structopt::StructOpt;
use colored::*;
use serde_json::{Value};

use crate::CliError;
use super::*;

// constants
const MIN_KUBE_VERSION: &'static str = "1.5.0";
const DEFAULT_HELM_VERSION: &'static str = "3.2.0";
const SYS_CHART_VERSION: &'static str = "0.1.0";
const SYS_CHART_NAME: &'static str = "fluvio-sys";
const DEFAULT_NAMESPACE: &'static str = "default";
const DUMMY_LB_SERVICE: &'static str = "flv-dummy-service";
const RESOURCE_SERVICE: &'static str = "service";
const RESOURCE_CRD: &'static str = "customresourcedefinitions";
const RESOURCE_SERVICE_ACCOUNT: &'static str = "secret";

#[derive(Debug, StructOpt)]
pub struct CheckCommand {
    /// run pre-install checks
    #[structopt(long)]
    pre_install: bool,
}

// describe all checks needed pre installation in an enum
#[derive(Debug)]
enum InstallCheck {
    LoadableConfig,
    K8Version,
    HelmVersion,
    SysChart,
    LoadBalancer,
    CreateServicePermission,
    CreateCrdPermission,
    CreateServiceAccountPermission,
}

// install check details such as success and failure message
struct InstallCheckObject {
    name: InstallCheck,
    success_message: &'static str,
    failed_message: &'static str,
}

pub async fn run_checks(opt: CheckCommand) -> Result<String, CliError> {
    if opt.pre_install {
        run_preinstall_checks().await?;
    }
    Ok("".to_string())
}

async fn run_preinstall_checks() -> Result<(), CliError> {
    // List of checks
    let pre_install_checks: [InstallCheckObject; 8] = [
        InstallCheckObject {
            name: InstallCheck::LoadableConfig,
            success_message:
                "Kubernetes config is loadable and cluster hostname is not an IP address",
            failed_message: "Failed to load kubernetes config",
        },
        InstallCheckObject {
            name: InstallCheck::K8Version,
            success_message: "Supported kubernetes version is installed",
            failed_message: "Supported kubernetes version is not installed",
        },
        InstallCheckObject {
            name: InstallCheck::HelmVersion,
            success_message: "Supported helm version is installed",
            failed_message: "Supported helm version is not installed, > v3.2.0 is required",
        },
        InstallCheckObject {
            name: InstallCheck::SysChart,
            success_message: "Fluvio system charts are installed",
            failed_message: "Compatible fluvio system charts are not installed",
        },
        InstallCheckObject {
            name: InstallCheck::CreateServicePermission,
            success_message: "Can create a Service",
            failed_message: "Cannot create a Service",
        },
        InstallCheckObject {
            name: InstallCheck::CreateCrdPermission,
            success_message: "Can create CustomResourceDefinitions",
            failed_message: "Cannot create CustomResourceDefinitions",
        },
        InstallCheckObject {
            name: InstallCheck::CreateServiceAccountPermission,
            success_message: "Can create ServiceAccounts",
            failed_message: "Cannot create ServiceAccounts",
        },
        InstallCheckObject {
            name: InstallCheck::LoadBalancer,
            success_message: "Load Balancer is up",
            failed_message: "Load Balancer is down",
        },
    ];
    // capture failures if any
    let mut failures = Vec::with_capacity(4);
    println!("\nRunning pre-install checks....\n");

    // loop through checks
    for check in pre_install_checks.iter() {
        // perform check and catch the result
        let res = match check.name {
            InstallCheck::LoadableConfig => check_loadable_config(),
            InstallCheck::K8Version => check_version(),
            InstallCheck::HelmVersion => check_helm_version(),
            InstallCheck::SysChart => check_sys_charts(),
            InstallCheck::LoadBalancer => check_load_balancer_status().await,
            InstallCheck::CreateServicePermission => check_permission(RESOURCE_SERVICE),
            InstallCheck::CreateCrdPermission => check_permission(RESOURCE_CRD),
            InstallCheck::CreateServiceAccountPermission => {
                check_permission(RESOURCE_SERVICE_ACCOUNT)
            }
        };
        // show success or failure message dereived from install check object depending on result
        match res {
            Ok(_) => {
                let msg = format!("ok: {}", check.success_message);
                println!("✔️  {}", msg.green());
            }
            Err(err) => {
                let msg = format!(
                    "failed: {} \n      error:{}",
                    check.failed_message,
                    err.to_string()
                );
                println!("❌ {}", msg.red());
                failures.push(msg);
            }
        }
    }

    // check if there are any failures and show final message
    if failures.len() > 0 {
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
    return match k8_config {
        K8Config::Pod(_) => {
            return Err(IoError::new(
                ErrorKind::Other,
                "Pod config is not valid here",
            ))
        }
        K8Config::KubeConfig(config) => Ok(config),
    };
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
        Ok(url.host().unwrap().to_string())
    } else {
        Err(IoError::new(ErrorKind::Other, "no context found"))
    }
}

fn compute_user_name() -> Result<String, IoError> {
    let kc_config = get_current_context()?;

    if let Some(ctx) = kc_config.config.current_context() {
        Ok(ctx.context.user.to_owned())
    } else {
        Err(IoError::new(ErrorKind::Other, "no context found"))
    }
}

async fn wait_for_service_exist(ns: &str) -> Result<Option<String>, ClientError> {
    use k8_metadata_client::MetadataClient;

    let client = load_and_share()?;

    let input = InputObjectMeta::named(DUMMY_LB_SERVICE, ns);

    for _ in 0..10u16 {
        match client.retrieve_item::<ServiceSpec, _>(&input).await {
            Ok(svc) => {
                // check if load balancer status exists
                if let Some(addr) = svc.status.load_balancer.find_any_ip_or_host() {
                    return Ok(Some(addr.to_owned()));
                } else {
                    sleep(Duration::from_millis(3000)).await;
                }
            }
            Err(err) => match err {
                K8ClientError::NotFound => {
                    sleep(Duration::from_millis(3000)).await;
                }
                _ => assert!(false, format!("error: {}", err)),
            },
        };
    }

    Ok(None)
}

async fn check_load_balancer_status() -> Result<(), IoError> {
    let username = match compute_user_name() {
        Ok(username) => username,
        Err(e) => {
            return Err(IoError::new(
                ErrorKind::Other,
                format!("error fetching username from context {}", e.to_string()),
            ))
        }
    };
    if username == "minikube" {
        // create dummy service
        create_dummy_service()?;
        if let Some(_) = wait_for_service_exist(DEFAULT_NAMESPACE)
            .await
            .map_err(|err| IoError::new(ErrorKind::Other, err.to_string()))?
        {
            // IP found, everything good
            delete_service()?;
        } else {
            delete_service()?;
            return Err(IoError::new(
                ErrorKind::Other,
                format!("Not able to find the tunnel, please ensure minikube tunnel is up"),
            ));
        }
    }

    Ok(())
}

fn create_dummy_service() -> Result<(), IoError> {
    Command::new("kubectl")
        .arg("create")
        .arg("service")
        .arg("loadbalancer")
        .arg(DUMMY_LB_SERVICE)
        .arg("--tcp=5678:8080")
        .output()
        .map_err(|err| {
            IoError::new(
                ErrorKind::Other,
                format!("Error creating loadbalancer server: {}", err.to_string()),
            )
        })?;
    Ok(())
}

fn delete_service() -> Result<(), IoError> {
    Command::new("kubectl")
        .arg("delete")
        .arg("service")
        .arg(DUMMY_LB_SERVICE)
        .output()
        .map_err(|err| {
            IoError::new(
                ErrorKind::Other,
                format!("Error deleting loadbalancer server: {}", err.to_string()),
            )
        })?;
    Ok(())
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
        match IpAddr::from_str(&server_host) {
            Ok(_) => {
                return Err(IoError::new(ErrorKind::Other,
                    format!("Cluster in kube context cannot use IP address, please use minikube context: {}", server_host),
                ));
            }
            Err(_) => {
                // ignore as it is expected to be a non IP address
            }
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
    let sys_charts = install::installed_sys_charts(SYS_CHART_NAME);
    if sys_charts.len() == 1 {
        let installed_chart = sys_charts.first().unwrap();
        let installed_chart_version = installed_chart.app_version.clone();
        // checking version of chart found
        if Version::parse(&installed_chart_version) < Version::parse(SYS_CHART_VERSION) {
            return Err(IoError::new(ErrorKind::Other, format!(
                "Fluvio system chart {} is not compatible with fluvio platform, please install version >= {}",
                installed_chart_version, SYS_CHART_VERSION
            )));
        }
    } else if sys_charts.len() == 0 {
        return Err(IoError::new(
            ErrorKind::Other,
            format!("Fluvio system chart not found, please install fluvio-sys first"),
        ));
    } else {
        return Err(IoError::new(
            ErrorKind::Other,
            format!("Multiple fluvio system charts found"),
        ));
    }
    Ok(())
}
