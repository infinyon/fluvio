use std::ops::Deref;

use fluvio_controlplane_metadata::core::MetadataItem;
use fluvio_types::SpuId;

use crate::stores::MetadataStoreObject;
use crate::stores::spg::SpuGroupSpec;
use crate::stores::spu::is_conflict;
use crate::stores::k8::K8MetaItem;
use crate::stores::spu::{SpuSpec};
use crate::stores::{LocalStore};
use crate::stores::actions::WSAction;

use crate::cli::TlsConfig;

use super::spu_k8_config::ScK8Config;
use super::statefulset::{StatefulsetSpec,StatefulsetStatus, K8StatefulSetSpec};
use super::spg_service::SpgServiceSpec;

pub struct SpuGroupObj {
    inner: MetadataStoreObject<SpuGroupSpec,K8MetaItem>,
    svc_name: String
}

impl Deref for SpuGroupObj {
    type Target = MetadataStoreObject<SpuGroupSpec,K8MetaItem>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl SpuGroupObj {

    pub fn new(inner: MetadataStoreObject<SpuGroupSpec,K8MetaItem>) -> Self {

        let svc_name = format!("fluvio-spg-{}", inner.key());
        Self {
            inner,
            svc_name
        }
    }

    pub fn is_already_valid(&self) -> bool {
        self.status().is_already_valid()
    }

    pub async fn is_conflict_with(&self, spu_store: &LocalStore<SpuSpec,K8MetaItem>) -> Option<SpuId> {
        if self.is_already_valid() {
            return None;
        }

        let min_id = self.spec.min_id as SpuId;

        is_conflict(
            spu_store,
            self.ctx().item().uid().clone(),
            min_id,
            min_id + self.spec.replicas as SpuId,
        )
        .await
    }

    /// convert SpuGroup to Statefulset Name and Spec
    pub fn statefulset_action(
        &self,
        namespace: &str,
        spu_k8_config: &ScK8Config,
        tls: Option<&TlsConfig>,
    ) -> WSAction<StatefulsetSpec>
    {
        let statefulset_name = format!("fluvio-spg-{}", self.key());
        let k8_spec = k8_convert::generate_k8_stateful(
            &self.spec,
            self.key(),
            &self.svc_name,
            namespace,
            spu_k8_config,
            tls,
        );

        WSAction::UpdateSpec((self.key().to_owned(),k8_spec.into()))
        
    }


    
    pub fn generate_service(
        &self,
        service_name: &str
    ) -> WSAction<SpgServiceSpec> {

        let k8_service = k8_convert::generate_service(self.spec(),service_name);

        WSAction::UpdateSpec((self.key().to_owned(),k8_service.into()))

    }



}


mod k8_convert {

    use std::collections::HashMap;

    use k8_types::*;
    use k8_types::core::pod::{ ContainerSpec,ContainerPortSpec,PodSpec,VolumeMount,VolumeSpec,SecretVolumeSpec};
    use k8_types::core::service::*;
    use k8_types::app::stateful::{
        PersistentVolumeClaim, VolumeAccessMode, ResourceRequirements, VolumeRequest,
    };
    use fluvio_types::defaults::{ SPU_DEFAULT_NAME,SPU_PUBLIC_PORT,SPU_PRIVATE_PORT,SC_PRIVATE_PORT,PRODUCT_NAME};

    use crate::stores::spg::SpuGroupSpec;
    use super::super::statefulset::{K8StatefulSetSpec};
    use super::{ ScK8Config,TlsConfig};

    /// convert spu group spec into k8 statefulset spec
    pub fn generate_k8_stateful(
        spg_spec: &SpuGroupSpec,
        name: &str,
        group_svc_name: &str,
        namespace: &str,
        spu_k8_config: &ScK8Config,
        tls_config: Option<&TlsConfig>,
    ) -> K8StatefulSetSpec {
        let replicas = spg_spec.replicas;
        let spu_template = &spg_spec.spu_config;
        let mut public_port = ContainerPortSpec {
            container_port: SPU_PUBLIC_PORT,
            ..Default::default()
        };
        public_port.name = Some("public".to_owned());

        let mut private_port = ContainerPortSpec {
            container_port: SPU_PRIVATE_PORT,
            ..Default::default()
        };
        private_port.name = Some("private".to_owned());

        // storage is special because defaults are explicit.
        let storage = spu_template.real_storage_config();
        let size = storage.size;
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
            "/fluvio".to_owned(),
            "run".to_owned(),
            "spu".to_owned(),
            "--sc-addr".to_owned(),
            format!(
                "fluvio-sc-internal.{}.svc.cluster.local:{}",
                namespace, SC_PRIVATE_PORT
            ),
            "--log-base-dir".to_owned(),
            storage.log_dir,
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

       // env.append(&mut spu_template.env.clone());

        let template = TemplateSpec {
            metadata: Some(
                TemplateMeta::default().set_labels(vec![("app", SPU_DEFAULT_NAME), ("group", name)]),
            ),
            spec: PodSpec {
                termination_grace_period_seconds: Some(10),
                containers: vec![ContainerSpec {
                    name: SPU_DEFAULT_NAME.to_owned(),
                    image: Some(spu_k8_config.image.clone()),
                    resources: spu_k8_config.resources.clone(),
                    ports: vec![public_port, private_port],
                    volume_mounts,
                    env,
                    args,
                    ..Default::default()
                }],
                volumes,
                security_context: spu_k8_config.pod_security_context.clone(),
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

        K8StatefulSetSpec {
            replicas: Some(replicas),
            service_name: group_svc_name.to_owned(),
            selector: LabelSelector::new_labels(vec![("app", SPU_DEFAULT_NAME), ("group", name)]),
            template,
            volume_claim_templates: vec![TemplateSpec {
                spec: claim,
                metadata: Some(TemplateMeta::named("data")),
            }],
            ..Default::default()
        }
    }

    /// generate headless service from SPG spec
    /// for now, we forgo port and env variable because it wasn't mapped from K8
    pub fn generate_service(spg: &SpuGroupSpec, name: &str) -> ServiceSpec {
        let spu_template = &spg.spu_config;
        let mut public_port = ServicePort {
            port: SPU_PUBLIC_PORT,
            ..Default::default()
        };

        public_port.name = Some("public".to_owned());
        let mut private_port = ServicePort {
            port: SPU_PRIVATE_PORT,
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
}