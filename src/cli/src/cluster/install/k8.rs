use std::process::Command;
use std::io::Error as IoError;
use semver::Version;
use k8_client::K8Config;
use k8_config::KubeContext;
use std::io::ErrorKind;
use std::net::IpAddr;
use std::str::FromStr;
use url::Url;
use tracing::*;

use super::*;
use crate::target::ClusterTarget;
use crate::tls::TlsOpt;

const CORE_CHART_NAME: &str = "fluvio/fluvio-core";

fn get_cluster_server_host(kc_config: KubeContext) -> Result<String, IoError> {
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

    const DEFAULT_HELM_VERSION: &str = "3.2.0";

    if Version::parse(&version_text_trimmed) < Version::parse(DEFAULT_HELM_VERSION) {
        return Err(CliError::Other(format!(
            "Helm version {} is not compatible with fluvio platform, please install version >= {}",
            version_text_trimmed, DEFAULT_HELM_VERSION
        )));
    }

    const SYS_CHART_VERSION: &str = "0.1.0";
    const SYS_CHART_NAME: &str = "fluvio-sys";

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
    } else if sys_charts.is_empty() {
        return Err(CliError::Other(
            "Fluvio system chart is not installed, please install fluvio-sys first".to_string(),
        ));
    } else {
        return Err(CliError::Other(
            "Multiple fluvio system charts found".to_string(),
        ));
    }

    let k8_config = K8Config::load()?;

    match k8_config {
        K8Config::Pod(_) => {
            // ignore server check for pod
        }
        K8Config::KubeConfig(config) => {
            let server_host = match get_cluster_server_host(config) {
                Ok(server) => server,
                Err(e) => {
                    return Err(CliError::Other(format!(
                        "error fetching server from kube context {}",
                        e.to_string()
                    )))
                }
            };
            if !server_host.trim().is_empty() {
                if IpAddr::from_str(&server_host).is_ok() {
                    return Err(CliError::Other(
                        format!("Cluster in kube context cannot use IP address, please use minikube context: {}", server_host),
                    ));
                };
            } else {
                return Err(CliError::Other(
                    "Cluster in kubectl context cannot have empty hostname".to_owned(),
                ));
            }
        }
    };

    Ok(())
}

pub async fn install_core(opt: InstallCommand) -> Result<(), CliError> {
    pre_install_check().map_err(|err| CliError::Other(err.to_string()))?;
    install_core_app(&opt)?;

    if k8_util::wait_for_service_exist(&opt.k8_config.namespace)
        .await
        .map_err(|err| CliError::Other(err.to_string()))?
        .is_some()
    {
        info!("fluvio is up");

        let external_addr =
            match crate::profile::discover_fluvio_addr(Some(&opt.k8_config.namespace)).await? {
                Some(sc_addr) => sc_addr,
                None => {
                    return Err(CliError::Other(
                        "fluvio service is not deployed".to_string(),
                    ))
                }
            };

        debug!("found sc, addr is: {}", external_addr);

        let tls_config = &opt.tls;
        let tls = if tls_config.tls {
            TlsOpt {
                tls: true,
                domain: tls_config.domain.clone(),
                enable_client_cert: true,
                client_key: tls_config.client_key.clone(),
                client_cert: tls_config.client_cert.clone(),
                ca_cert: tls_config.ca_cert.clone(),
            }
        } else {
            TlsOpt::default()
        };

        if !opt.skip_profile_creation {
            set_profile(
                external_addr.clone(),
                tls.clone(),
                Some(opt.k8_config.namespace.clone()),
            )
            .await?;
        }

        if opt.spu > 0 {
            use std::time::Duration;
            use flv_future_aio::timer::sleep;

            // wait little bit for sc to spin up

            sleep(Duration::from_millis(2000)).await;

            create_spg(
                ClusterTarget {
                    cluster: Some(external_addr),
                    tls,
                    ..Default::default()
                },
                &opt,
            )
            .await?;

            if !k8_util::wait_for_spu(&opt.k8_config.namespace).await? {
                println!("too long to wait for spu to be ready");
                return Err(CliError::Other(
                    "unable to detect fluvio service".to_owned(),
                ));
            }

            // print spu
            let _ = Command::new("kubectl")
                .arg("get")
                .arg("spu")
                .print()
                .inherit();
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
        opt.k8_config.version.clone().unwrap_or_else(|| {
            // get git version
            let output = Command::new("git")
                .args(&["log", "-1", "--pretty=format:\"%H\""])
                .output()
                .unwrap();
            let version = String::from_utf8(output.stdout).unwrap();
            version.trim_matches('"').to_owned()
        })
    } else {
        helm::repo_add(opt.k8_config.chart_location.as_deref());
        helm::repo_update();
        crate::VERSION.to_owned()
    };

    let k8_config = &opt.k8_config;
    let ns = &k8_config.namespace;

    println!("installing fluvio chart version: {}", version);

    let fluvio_version = format!("fluvioVersion={}", version);

    let mut cmd = Command::new("helm");

    let registry = k8_config.registry.as_deref().unwrap_or({
        if opt.develop {
            "localhost:5000/infinyon"
        } else {
            "infinyon"
        }
    });

    // prepare chart if using release
    if !opt.develop {
        debug!("updating helm repo");
        const CORE_CHART_NAME: &str = "fluvio/fluvio-core";
        helm::repo_add(opt.k8_config.chart_location.as_deref());
        helm::repo_update();

        if !helm::check_chart_version_exists(CORE_CHART_NAME, crate::VERSION) {
            return Err(CliError::Other(format!(
                "{}:{} not found in helm repo",
                CORE_CHART_NAME,
                crate::VERSION
            )));
        }
    }

    if opt.develop {
        cmd.arg("install").arg(&k8_config.install_name).arg(
            k8_config
                .chart_location
                .as_deref()
                .unwrap_or("./k8-util/helm/fluvio-core"),
        );
    } else {
        cmd.arg("install")
            .arg(&k8_config.install_name)
            .arg(CORE_CHART_NAME);
    }

    cmd.arg("--set")
        .arg(fluvio_version)
        .arg("--set")
        .arg(format!("registry={}", registry))
        .arg("-n")
        .arg(ns)
        .arg("--set")
        .arg(format!("cloud={}", k8_config.cloud));

    if opt.tls.tls {
        cmd.arg("--set").arg("tls=true");
    }

    if let Some(log) = &opt.rust_log {
        cmd.arg("--set").arg(format!("scLog={}", log));
    }

    cmd.wait();

    println!("fluvio chart has been installed");

    Ok(())
}

pub fn install_sys(opt: InstallCommand) {
    helm::repo_add(opt.k8_config.chart_location.as_deref());
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
async fn set_profile(
    external_addr: String,
    tls: TlsOpt,
    namespace: Option<String>,
) -> Result<(), CliError> {
    use crate::profile::set_k8_context;
    use crate::profile::K8Opt;

    let config = K8Opt {
        namespace,
        tls,
        ..Default::default()
    };

    println!(
        "updated profile: {:#?}",
        set_k8_context(config, external_addr).await?
    );

    Ok(())
}

async fn create_spg(target: ClusterTarget, opt: &InstallCommand) -> Result<(), CliError> {
    use crate::group::process_create_managed_spu_group;
    use crate::group::CreateManagedSpuGroupOpt;

    let group_name = &opt.k8_config.group_name;
    let group_opt = CreateManagedSpuGroupOpt {
        name: group_name.clone(),
        replicas: opt.spu,
        target,
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
    use k8_client::K8Client;
    use k8_obj_core::service::ServiceSpec;
    use k8_obj_metadata::InputObjectMeta;
    use k8_metadata_client::MetadataClient;
    use k8_client::ClientError as K8ClientError;

    use super::*;

    /// print svc
    fn print_svc(ns: &str) {
        Command::new("kubectl")
            .arg("get")
            .arg("svc")
            .arg("-n")
            .arg(ns)
            .inherit();
    }

    pub async fn wait_for_service_exist(ns: &str) -> Result<Option<String>, ClientError> {
        let client = K8Client::default()?;

        let input = InputObjectMeta::named("flv-sc-public", ns);

        for i in 0..100u16 {
            println!("checking to see if svc exists, count: {}", i);
            match client.retrieve_item::<ServiceSpec, _>(&input).await {
                Ok(svc) => {
                    // check if load balancer status exists
                    if let Some(addr) = svc.status.load_balancer.find_any_ip_or_host() {
                        print!("found svc load balancer addr: {}", addr);
                        print_svc(ns);
                        return Ok(Some(format!("{}:9003", addr.to_owned())));
                    } else {
                        println!("svc exists but no load balancer exist yet, continue wait");
                        sleep(Duration::from_millis(2000)).await;
                    }
                }
                Err(err) => match err {
                    K8ClientError::NotFound => {
                        println!("no svc found, sleeping ");
                        sleep(Duration::from_millis(2000)).await;
                    }
                    _ => panic!("error: {}", err),
                },
            };
        }

        // if we  can't  find any service print out kc get svc
        print_svc(ns);

        Ok(None)
    }

    /// wait until all spus are ready and have ingres
    pub async fn wait_for_spu(ns: &str) -> Result<bool, ClientError> {
        use flv_metadata_cluster::spu::SpuSpec;

        let client = K8Client::default()?;

        for i in 0..100u16 {
            let items = client.retrieve_items::<SpuSpec, _>(ns).await?;
            let spu_count = items.items.len();
            // check for all items has ingress

            let ready_spu = items
                .items
                .iter()
                .filter(|spu_obj| {
                    !spu_obj.spec.public_endpoint.ingress.is_empty() && spu_obj.status.is_online()
                })
                .count();

            if spu_count == ready_spu {
                println!("all spu: {} is ready", spu_count);
                return Ok(true);
            } else {
                println!(
                    "spu: {} out of {} ready, waiting: {}",
                    ready_spu, spu_count, i
                );
                sleep(Duration::from_millis(2000)).await;
            }
        }

        Ok(false)
    }
}
