use std::time::Duration;
use std::process::Stdio;
use std::io::Error as IoError;

use tracing::debug;

use k8_client::SharedK8Client;
use fluvio_future::timer::sleep;

use crate::CliError;

use super::InstallCommand;
use super::CommandUtil;

#[cfg(target_os = "macos")]
fn get_log_directory() -> &'static str {
    "/usr/local/var/log/fluvio"
}

#[cfg(not(target_os = "macos"))]
fn get_log_directory() -> &'static str {
    "/var/log/fluvio"
}

pub async fn install_local(opt: InstallCommand) -> Result<(), CliError> {
    use std::path::Path;
    use std::fs::create_dir_all;

    let log_dir = opt
        .log_dir
        .clone()
        .unwrap_or_else(|| get_log_directory().to_owned());

    debug!("using log dir: {}", log_dir);

    if !Path::new(&log_dir).exists() {
        create_dir_all(&log_dir).map_err(CliError::IoError)?;
    }

    // ensure we sync files before we launch servers
    Command::new("sync").inherit();

    println!("launching sc");
    launch_sc(&opt, &log_dir);

    println!("setting local profile");
    set_profile(&opt)?;

    println!("launching spu group with size: {}", opt.spu);
    launch_spu_group(&opt, &log_dir).await;

    sleep(Duration::from_secs(1)).await;

    Ok(())
}

fn launch_sc(option: &InstallCommand, log_dir: &str) {
    use std::fs::File;

    let outputs = File::create(format!("{}/flv_sc.log", log_dir)).expect("log file");
    let errors = outputs.try_clone().expect("error  file");

    debug!("starting sc server");

    #[cfg(not(feature = "cluster_components"))]
    let mut binary = super::get_binary("fluvio-sc-k8").expect("unable to get sc-server");

    #[cfg(feature = "cluster_components")]
    let mut binary = {
        let mut cmd =
            Command::new(std::env::current_exe().expect("unable to get current executable"));
        cmd.arg("run");
        cmd.arg("sc");
        cmd
    };

    if option.tls.tls {
        set_server_tls(&mut binary, option, 9005);
    }

    if let Some(log) = &option.rust_log {
        binary.env("RUST_LOG", log);
    }
    binary.print();

    binary
        .stdout(Stdio::from(outputs))
        .stderr(Stdio::from(errors))
        .spawn()
        .expect("sc server failed to start");
}

/// set local profile
fn set_profile(opt: &InstallCommand) -> Result<(), IoError> {
    use crate::profile::set_local_context;
    use crate::tls::TlsClientOpt;

    let tls_config = &opt.tls;
    let tls = if tls_config.tls {
        TlsClientOpt {
            tls: true,
            domain: tls_config.domain.clone(),
            enable_client_cert: true,
            client_key: tls_config.client_key.clone(),
            client_cert: tls_config.client_cert.clone(),
            ca_cert: tls_config.ca_cert.clone(),
        }
    } else {
        TlsClientOpt::default()
    };

    let local = LocalOpt {
        local: "localhost:9003".to_owned(),
        tls,
    };

    println!("{}", set_local_context(local)?);

    Ok(())
}

async fn launch_spu_group(opt: &InstallCommand, log_dir: &str) {
    use k8_client::load_and_share;

    let client = load_and_share().expect("client should not fail");

    for i in 0..opt.spu {
        println!("launching SPU ({} of {})", i + 1, opt.spu);
        launch_spu(i, client.clone(), opt, log_dir).await;
    }
    println!("SC log generated at {}/flv_sc.log", log_dir);
    sleep(Duration::from_millis(500)).await;
}

async fn launch_spu(
    spu_index: u16,
    client: SharedK8Client,
    option: &InstallCommand,
    log_dir: &str,
) {
    use std::fs::File;

    use fluvio::metadata::spu::SpuSpec;
    use fluvio::metadata::spu::IngressPort;
    use fluvio::metadata::spu::Endpoint;
    use fluvio::metadata::spu::IngressAddr;
    use k8_obj_metadata::InputK8Obj;
    use k8_obj_metadata::InputObjectMeta;
    use k8_metadata_client::MetadataClient;

    const BASE_PORT: u16 = 9010;
    const BASE_SPU: u16 = 5001;

    let spu_id = (BASE_SPU + spu_index) as i32;
    let public_port = BASE_PORT + spu_index * 10;
    let private_port = public_port + 1;

    let spu_spec = SpuSpec {
        id: spu_id,
        public_endpoint: IngressPort {
            port: public_port,
            ingress: vec![IngressAddr {
                hostname: Some("localhost".to_owned()),
                ..Default::default()
            }],
            ..Default::default()
        },
        private_endpoint: Endpoint {
            port: private_port,
            host: "localhost".to_owned(),
            ..Default::default()
        },
        ..Default::default()
    };

    let input = InputK8Obj::new(
        spu_spec,
        InputObjectMeta {
            name: format!("custom-spu-{}", spu_id),
            namespace: "default".to_owned(),
            ..Default::default()
        },
    );

    client.create_item(input).await.expect("item created");

    // sleep 1 seconds for sc to connect
    sleep(Duration::from_millis(300)).await;

    let log_spu = format!("{}/spu_log_{}.log", log_dir, spu_id);
    let outputs = File::create(&log_spu).expect("log file");
    let errors = outputs.try_clone().expect("error  file");

    #[cfg(not(feature = "cluster_components"))]
    let mut binary = get_binary("fluvio-spu").expect("unable to get fluvio-spu");

    #[cfg(feature = "cluster_components")]
    let mut binary = {
        let mut cmd =
            Command::new(std::env::current_exe().expect("unable to get current executable"));
        cmd.arg("run");
        cmd.arg("spu");
        cmd
    };

    if option.tls.tls {
        set_server_tls(&mut binary, option, private_port + 1);
    }

    if let Some(log) = &option.rust_log {
        binary.env("RUST_LOG", log);
    }

    let cmd = binary
        .arg("-i")
        .arg(format!("{}", spu_id))
        .arg("-p")
        .arg(format!("0.0.0.0:{}", public_port))
        .arg("-v")
        .arg(format!("0.0.0.0:{}", private_port))
        .print();

    println!("SPU<{}> cmd: {:#?}", spu_index, cmd);
    println!("SPU log generated at {}", log_spu);
    cmd.stdout(Stdio::from(outputs))
        .stderr(Stdio::from(errors))
        .spawn()
        .expect("spu server failed to start");
}

use std::process::Command;
use crate::profile::LocalOpt;

fn set_server_tls(cmd: &mut Command, option: &InstallCommand, port: u16) {
    let tls = &option.tls;

    println!("starting SC with TLS options");

    cmd.arg("--tls")
        .arg("--enable-client-cert")
        .arg("--server-cert")
        .arg(&tls.server_cert.as_ref().expect("server cert"))
        .arg("--server-key")
        .arg(&tls.server_key.as_ref().expect("server key"))
        .arg("--ca-cert")
        .arg(&tls.ca_cert.as_ref().expect("ca cert"))
        .arg("--bind-non-tls-public")
        .arg(format!("0.0.0.0:{}", port));
}
