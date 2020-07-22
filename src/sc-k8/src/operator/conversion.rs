/// convert spu group spec to statefulset for input
use std::collections::HashMap;

use flv_metadata::k8::metadata::*;
use flv_metadata::k8::core::pod::ContainerSpec;
use flv_metadata::k8::core::pod::ContainerPortSpec;
use flv_metadata::k8::core::pod::PodSpec;
use flv_metadata::k8::core::pod::VolumeMount;
use flv_metadata::k8::core::pod::VolumeSpec;
use flv_metadata::k8::core::pod::SecretVolumeSpec;
use flv_metadata::k8::core::service::*;
use flv_metadata::k8::app::stateful::*;
use flv_metadata::spg::K8SpuGroupSpec;

use flv_types::defaults::SPU_DEFAULT_NAME;
use flv_types::defaults::SPU_PUBLIC_PORT;
use flv_types::defaults::SPU_PRIVATE_PORT;
use flv_types::defaults::SC_PRIVATE_PORT;
use flv_types::defaults::PRODUCT_NAME;

use crate::cli::TlsConfig;

fn find_spu_image() -> String {
    std::env::var("SPU_IMAGE").expect("SPU IMAGE must be passed as env")
}

/// convert SpuGroup to Statefulset
pub fn convert_cluster_to_statefulset(
    group_spec: &K8SpuGroupSpec,
    metadata: &ObjectMeta,
    group_name: &str,
    group_svc_name: String,
    namespace: &str,
    tls: Option<&TlsConfig>,
) -> InputK8Obj<StatefulSetSpec> {
    let statefulset_name = format!("flv-spg-{}", group_name);
    let spec = generate_stateful(group_spec, group_name, group_svc_name, namespace, tls);
    let owner_ref = metadata.make_owner_reference::<K8SpuGroupSpec>();

    InputK8Obj {
        api_version: StatefulSetSpec::api_version(),
        kind: StatefulSetSpec::kind(),
        metadata: InputObjectMeta {
            name: statefulset_name.clone(),
            namespace: metadata.namespace().to_string(),
            owner_references: vec![owner_ref],
            ..Default::default()
        },
        spec,
        ..Default::default()
    }
}

/// generate statefulset spec from cluster spec
fn generate_stateful(
    spg_spec: &K8SpuGroupSpec,
    name: &str,
    group_svc_name: String,
    namespace: &str,
    tls_config: Option<&TlsConfig>,
) -> StatefulSetSpec {
    let replicas = spg_spec.replicas;
    let spg_template = &spg_spec.template.spec;
    let mut public_port = ContainerPortSpec {
        container_port: spg_template
            .public_endpoint
            .as_ref()
            .map(|t| t.port)
            .unwrap_or(SPU_PUBLIC_PORT),
        ..Default::default()
    };
    public_port.name = Some("public".to_owned());

    let mut private_port = ContainerPortSpec {
        container_port: spg_template
            .private_endpoint
            .as_ref()
            .map(|t| t.port)
            .unwrap_or(SPU_PRIVATE_PORT),
        ..Default::default()
    };
    private_port.name = Some("private".to_owned());

    // storage is special because defaults are explicit.
    let storage = spg_spec.template.spec.storage.clone().unwrap_or_default();
    let size = storage.size();
    let mut env = vec![
        Env::key_field_ref("SPU_INDEX", "metadata.name"),
        Env::key_value("SPU_MIN", &format!("{}", spg_spec.min_id)),
    ];

    let mut volume_mounts = vec![VolumeMount {
        name: "data".to_owned(),
        mount_path: format!("/var/lib/{}/data", PRODUCT_NAME),
        ..Default::default()
    }];

    let mut volumes = vec![];

    let mut args = vec![
        "/fluvio/spu-server".to_owned(),
        "--sc-addr".to_owned(),
        format!(
            "flv-sc-internal.{}.svc.cluster.local:{}",
            namespace, SC_PRIVATE_PORT
        ),
        "--log-base-dir".to_owned(),
        storage.log_dir(),
        "--log-size".to_owned(),
        size.clone(),
    ];

    if let Some(tls) = tls_config {
        args.push("--tls".to_owned());
        if tls.enable_client_cert {
            args.push("--enable-client-cert".to_owned());
            args.push("--ca-cert".to_owned());
            args.push(tls.ca_cert.clone().unwrap());
            volume_mounts.push(VolumeMount {
                name: "cacert".to_owned(),
                mount_path: "/var/certs/ca".to_owned(),
                read_only: Some(true),
                ..Default::default()
            });
            volumes.push(VolumeSpec {
                name: "cacert".to_owned(),
                secret: Some(SecretVolumeSpec {
                    secret_name: "fluvio-ca".to_owned(), // fixed
                    ..Default::default()
                }),
                ..Default::default()
            });
        }

        args.push("--server-cert".to_owned());
        args.push(tls.server_cert.clone().unwrap());
        args.push("--server-key".to_owned());
        args.push(tls.server_key.clone().unwrap());

        volume_mounts.push(VolumeMount {
            name: "tls".to_owned(),
            mount_path: "/var/certs/tls".to_owned(),
            read_only: Some(true),
            ..Default::default()
        });

        volumes.push(VolumeSpec {
            name: "tls".to_owned(),
            secret: Some(SecretVolumeSpec {
                secret_name: "fluvio-tls".to_owned(), // fixed
                ..Default::default()
            }),
            ..Default::default()
        });

        args.push("--bind-non-tls-public".to_owned());
        args.push("0.0.0.0:9007".to_owned());
    }

    // add RUST LOG, if passed
    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        env.push(Env::key_value("RUST_LOG", &rust_log));
    }

    env.append(&mut spg_template.env.clone());

    let template = TemplateSpec {
        metadata: Some(
            TemplateMeta::default().set_labels(vec![("app", SPU_DEFAULT_NAME), ("group", name)]),
        ),
        spec: PodSpec {
            termination_grace_period_seconds: Some(10),
            containers: vec![ContainerSpec {
                name: SPU_DEFAULT_NAME.to_owned(),
                image: Some(find_spu_image()),
                ports: vec![public_port, private_port],
                volume_mounts,
                env,
                args,
                ..Default::default()
            }],
            volumes,
            ..Default::default()
        },
    };
    let claim = PersistentVolumeClaim {
        access_modes: vec![VolumeAccessMode::ReadWriteOnce],
        storage_class_name: format!("{}-{}", PRODUCT_NAME, SPU_DEFAULT_NAME),
        resources: ResourceRequirements {
            requests: VolumeRequest { storage: size },
        },
    };

    StatefulSetSpec {
        replicas: Some(replicas),
        service_name: group_svc_name,
        selector: LabelSelector::new_labels(vec![("app", SPU_DEFAULT_NAME), ("group", name)]),
        template,
        volume_claim_templates: vec![TemplateSpec {
            spec: claim,
            metadata: Some(TemplateMeta::named("data")),
        }],
        ..Default::default()
    }
}

pub fn generate_service(spg: &K8SpuGroupSpec, name: &str) -> ServiceSpec {
    let spg_template = &spg.template.spec;
    let mut public_port = ServicePort {
        port: spg_template
            .public_endpoint
            .as_ref()
            .map(|t| t.port)
            .unwrap_or(SPU_PUBLIC_PORT),
        ..Default::default()
    };

    public_port.name = Some("public".to_owned());
    let mut private_port = ServicePort {
        port: spg_template
            .private_endpoint
            .as_ref()
            .map(|t| t.port)
            .unwrap_or(SPU_PRIVATE_PORT),
        ..Default::default()
    };
    private_port.name = Some("private".to_owned());

    let mut selector = HashMap::new();
    selector.insert("app".to_owned(), SPU_DEFAULT_NAME.to_owned());
    selector.insert("group".to_owned(), name.to_owned());

    ServiceSpec {
        cluster_ip: "None".to_owned(),
        ports: vec![public_port, private_port],
        selector: Some(selector),
        ..Default::default()
    }
}
