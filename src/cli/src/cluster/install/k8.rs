use std::process::Command;
use std::io::Error as IoError;
use semver::Version;
use k8_client::K8Config;
use std::io::ErrorKind;
use std::net::{IpAddr};
use std::str::FromStr;
use url::{Url};

use super::*;

fn get_cluster_server_host() -> Result<String, IoError> {
    let k8_config = K8Config::load().map_err(|err| {
        IoError::new(
            ErrorKind::Other,
            format!("unable to load kube context {}", err),
        )
    })?;
    let kc_config = match k8_config {
        K8Config::Pod(_) => {
            return Err(IoError::new(
                ErrorKind::Other,
                "Pod config is not valid here",
            ))
        }
        K8Config::KubeConfig(config) => config,
    };

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

fn pre_install_check() -> Result<(), CliError> {
    let helm_version = Command::new("helm")
        .arg("version")
        .arg("--short")
        .output()
        .map_err(|err| {
            CliError::Other(format!(
                "Helm package manager not found: {}",
                err.to_string()
            ))
        })?;
    let version_text = String::from_utf8(helm_version.stdout).unwrap();
    let version_text_trimmed = &version_text[1..].trim();

    const DEFAULT_HELM_VERSION: &'static str = "3.2.0";

    if Version::parse(&version_text_trimmed) < Version::parse(DEFAULT_HELM_VERSION) {
        return Err(CliError::Other(format!(
            "Helm version {} is not compatible with fluvio platform, please install version >= {}",
            version_text_trimmed, DEFAULT_HELM_VERSION
        )));
    }

    const SYS_CHART_VERSION: &'static str = "0.1.0";
    const SYS_CHART_NAME: &'static str = "fluvio-sys";

    let sys_charts = helm::installed_sys_charts(SYS_CHART_NAME);
    if sys_charts.len() == 1 {
        let installed_chart = sys_charts.first().unwrap();
        let installed_chart_version = installed_chart.app_version.clone();
        // checking version of chart found
        if Version::parse(&installed_chart_version) < Version::parse(SYS_CHART_VERSION) {
            return Err(CliError::Other(format!(
                "Fluvio system chart {} is not compatible with fluvio platform, please install version >= {}",
                installed_chart_version, SYS_CHART_VERSION
            )));
        }
    } else if sys_charts.len() == 0 {
        return Err(CliError::Other(format!(
            "Fluvio system chart is not installed, please install fluvio-sys first",
        )));
    } else {
        return Err(CliError::Other(format!(
            "Multiple fluvio system charts found",
        )));
    }

    let server_host = match get_cluster_server_host() {
        Ok(server) => server,
        Err(e) => {
            return Err(CliError::Other(format!(
                "error fetching server from kube context {}",
                e.to_string()
            )))
        }
    };

    if !server_host.trim().is_empty() {
        match IpAddr::from_str(&server_host) {
            Ok(_) => {
                return Err(CliError::Other(
                    format!("Cluster in kube context cannot use IP address, please use minikube context: {}", server_host),
                ));
            }
            Err(_) => {
                // ignore as it is expected to be a non IP address
            }
        };
    } else {
        return Err(CliError::Other(
            "Cluster in kubectl context cannot have empty hostname".to_owned(),
        ));
    }

    Ok(())
}

pub async fn install_core(opt: InstallCommand) -> Result<(), CliError> {
    pre_install_check().map_err(|err| CliError::Other(err.to_string()))?;
    install_core_app(&opt)?;

    if let Some(_) = k8_util::wait_for_service_exist(&opt.k8_config.namespace)
        .await
        .map_err(|err| CliError::Other(err.to_string()))?
    {
        println!("fluvio is up");
        set_profile(&opt).await?;

        if opt.spu > 0 {
            create_spg(&opt).await?;
        }

        Ok(())
    } else {
        println!("unable to detect fluvio service");
        println!("for minikube, check if you have tunnel up!");
        Err(CliError::Other(
            "unable to detect fluvio service".to_owned(),
        ))
    }
}

/// for tls, copy secrets
fn copy_secrets(opt: &InstallCommand) {
    println!("copy secrets");

    let tls = &opt.tls;

    // copy certificate as kubernetes secrets

    Command::new("kubectl")
        .arg("create")
        .arg("secret")
        .arg("generic")
        .arg("fluvio-ca")
        .arg("--from-file")
        .arg(&tls.ca_cert.as_ref().expect("ca cert"))
        .inherit();

    Command::new("kubectl")
        .arg("create")
        .arg("secret")
        .arg("tls")
        .arg("fluvio-tls")
        .arg("--cert")
        .arg(&tls.server_cert.as_ref().expect("server cert"))
        .arg("--key")
        .arg(&tls.server_key.as_ref().expect("server key"))
        .inherit();
}

/// install helm core chart
fn install_core_app(opt: &InstallCommand) -> Result<(), CliError> {
    if opt.tls.tls {
        copy_secrets(opt);
    }

    let version = if opt.develop {
        // get git version
        let output = Command::new("git")
            .args(&["log", "-1", "--pretty=format:\"%H\""])
            .output()
            .unwrap();
        let version = String::from_utf8(output.stdout).unwrap();
        version.trim_matches('"').to_owned()
    } else {
        helm::repo_add();
        helm::repo_update();
        crate::VERSION.to_owned()
    };

    let registry = if opt.develop {
        "localhost:5000/infinyon"
    } else {
        "infinyon"
    };

    let k8_config = &opt.k8_config;
    let ns = &k8_config.namespace;

    println!("installing fluvio chart version: {}", version);

    let fluvio_version = format!("fluvioVersion={}", version);

    let mut cmd = Command::new("helm");

    if opt.develop {
        cmd.arg("install")
            .arg(&k8_config.name)
            .arg("./k8-util/helm/fluvio-core")
            .arg("--set")
            .arg(fluvio_version)
            .arg("--set")
            .arg(format!("registry={}", registry));
    } else {
        const CORE_CHART_NAME: &'static str = "fluvio/fluvio-core";
        helm::repo_add();
        helm::repo_update();

        if !helm::check_chart_version_exists(CORE_CHART_NAME, crate::VERSION) {
            return Err(CliError::Other(format!(
                "{}:{} not found in helm repo",
                CORE_CHART_NAME,
                crate::VERSION
            )));
        }

        cmd.arg("install")
            .arg(&k8_config.name)
            .arg(CORE_CHART_NAME)
            .arg("--version")
            .arg(crate::VERSION);
    };

    cmd.arg("-n")
        .arg(ns)
        .arg("--set")
        .arg(format!("cloud={}", k8_config.cloud));

    if opt.tls.tls {
        cmd.arg("--set").arg("tls=true");
    }

    if let Some(log) = &opt.log {
        cmd.arg("--set").arg(format!("scLog={}", log));
    }

    cmd.wait();

    println!("fluvio chart has been installed");

    Ok(())
}

pub fn install_sys(opt: InstallCommand) {
    helm::repo_add();
    helm::repo_update();

    Command::new("helm")
        .arg("install")
        .arg("fluvio-sys")
        .arg("fluvio/fluvio-sys")
        .arg("--set")
        .arg(format!("cloud={}", opt.k8_config.cloud))
        .inherit();
    println!("fluvio sys chart has been installed");
}

/// switch to profile
async fn set_profile(opt: &InstallCommand) -> Result<(), IoError> {
    use crate::profile::set_k8_context;
    use crate::profile::SetK8;
    use crate::tls::TlsConfig;

    let tls_config = &opt.tls;
    let tls = if tls_config.tls {
        TlsConfig {
            tls: true,
            domain: tls_config.domain.clone(),
            enable_client_cert: true,
            client_key: tls_config.client_key.clone(),
            client_cert: tls_config.client_cert.clone(),
            ca_cert: tls_config.ca_cert.clone(),
            ..Default::default()
        }
    } else {
        TlsConfig::default()
    };

    let config = SetK8 {
        namespace: Some(opt.k8_config.namespace.clone()),
        tls,
        ..Default::default()
    };

    println!("{}", set_k8_context(config).await?);

    Ok(())
}

async fn create_spg(opt: &InstallCommand) -> Result<(), CliError> {
    use crate::group::process_create_managed_spu_group;
    use crate::group::CreateManagedSpuGroupOpt;

    let group_name = &opt.k8_config.group_name;
    let group_opt = CreateManagedSpuGroupOpt {
        name: group_name.clone(),
        replicas: opt.spu,
        ..Default::default()
    };

    process_create_managed_spu_group(group_opt).await?;

    println!("group: {} with replica: {} created", group_name, opt.spu);

    Ok(())
}

mod k8_util {

    use std::time::Duration;

    use flv_future_aio::timer::sleep;
    use k8_client::ClientError;
    use k8_client::load_and_share;
    use k8_obj_core::service::ServiceSpec;
    use k8_obj_metadata::InputObjectMeta;
    use k8_metadata_client::MetadataClient;
    use k8_client::ClientError as K8ClientError;

    pub async fn wait_for_service_exist(ns: &str) -> Result<Option<String>, ClientError> {
        let client = load_and_share()?;

        let input = InputObjectMeta::named("flv-sc-public", ns);

        for i in 0..100u16 {
            println!("checking to see if svc exists, count: {}", i);
            match client.retrieve_item::<ServiceSpec, _>(&input).await {
                Ok(svc) => {
                    // check if load balancer status exists
                    if let Some(addr) = svc.status.load_balancer.find_any_ip_or_host() {
                        println!("found svc load balancer addr: {}", addr);
                        return Ok(Some(format!("{}:9003", addr.to_owned())));
                    } else {
                        println!("svc exists but no load balancer exist yet, continue wait");
                        sleep(Duration::from_millis(3000)).await;
                    }
                }
                Err(err) => match err {
                    K8ClientError::NotFound => {
                        println!("no svc found, sleeping ");
                        sleep(Duration::from_millis(3000)).await;
                    }
                    _ => assert!(false, format!("error: {}", err)),
                },
            };
        }

        Ok(None)
    }
}
