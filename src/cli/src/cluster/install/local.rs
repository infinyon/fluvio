use std::time::Duration;
use std::process::Stdio;
use std::io::Error as IoError;

use log::debug;

use k8_client::SharedK8Client;
use flv_future_aio::timer::sleep;

use crate::CliError;

use super::InstallCommand;
use super::get_binary;
use super::CommandUtil;

pub async fn install_local(opt: InstallCommand) -> Result<(), CliError> {
    println!("launching sc");
    launch_sc(&opt);

    println!("setting local profile");
    set_profile(&opt)?;

    println!("launching spu group with size: {}", opt.spu);
    launch_spu_group(&opt).await;

    sleep(Duration::from_secs(1)).await;

    Ok(())
}

fn launch_sc(option: &InstallCommand) {
    use std::fs::File;

    let outputs = File::create(format!("/tmp/flv_sc.log")).expect("log file");
    let errors = outputs.try_clone().expect("error  file");

    debug!("starting sc server");
    let mut base = get_binary("sc-k8-server").expect("unable to get sc-server");

    if option.tls.tls {
        set_server_tls(&mut base, option, 9005);
    }

    if let Some(log) = &option.log {
        base.env("RUST_LOG", log);
    }
    
    base.print();

    base.stdout(Stdio::from(outputs))
        .stderr(Stdio::from(errors))
        .spawn()
        .expect("sc server failed to start");
}

/// set local profile
fn set_profile(opt: &InstallCommand) -> Result<(), IoError> {
    use crate::profile::SetLocal;
    use crate::profile::set_local_context;
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

    let local = SetLocal {
        local: "localhost:9003".to_owned(),
        tls,
    };

    println!("{}", set_local_context(local)?);

    Ok(())
}

async fn launch_spu_group(opt: &InstallCommand) {
    use k8_client::load_and_share;

    let client = load_and_share().expect("client should not fail");

    for i in 0..opt.spu {
        println!("launching SPU ({} of {})", i+1, opt.spu);
        launch_spu(i, client.clone(), opt).await;
    }
    println!("SC log generated at /tmp/flv_sc.log");
    
    sleep(Duration::from_millis(500)).await;
}

async fn launch_spu(spu_index: u16, client: SharedK8Client, option: &InstallCommand) {
    use std::fs::File;

    use k8_metadata::spu::SpuSpec;
    use k8_metadata::spu::IngressPort;
    use k8_metadata::spu::Endpoint;
    use k8_metadata::spu::IngressAddr;
    use k8_obj_metadata::InputK8Obj;
    use k8_obj_metadata::InputObjectMeta;
    use k8_metadata_client::MetadataClient;

    const BASE_PORT: u16 = 9010;
    const BASE_SPU: u16 = 5001;

    let spu_id = (BASE_SPU + spu_index) as i32;
    let public_port = BASE_PORT + spu_index * 10;
    let private_port = public_port + 1;

    let spu_spec = SpuSpec {
        spu_id,
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

    let outputs = File::create(format!("/tmp/spu_log_{}.log", spu_id)).expect("log file");
    let errors = outputs.try_clone().expect("error  file");

    let mut binary = get_binary("spu-server").expect("unable to get spu-server");

    if option.tls.tls {
        set_server_tls(&mut binary, option, private_port + 1);
    }

    if let Some(log) = &option.log {
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
    println!("SPU log generated at /tmp/spu_log_{}.log", spu_id);
    cmd.stdout(Stdio::from(outputs))
        .stderr(Stdio::from(errors))
        .spawn()
        .expect("spu server failed to start");
}

use std::process::Command;

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
