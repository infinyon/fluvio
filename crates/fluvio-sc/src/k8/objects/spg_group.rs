use std::{collections::HashMap, ops::Deref};

use tracing::{trace, instrument, debug};

use fluvio_controlplane_metadata::core::MetadataItem;
use fluvio_types::SpuId;
use fluvio_controlplane_metadata::{
    spg::SpuEndpointTemplate,
    spu::{Endpoint, IngressPort, SpuType},
};

use crate::stores::MetadataStoreObject;
use crate::stores::spg::SpuGroupSpec;
use crate::stores::spu::is_conflict;
use crate::stores::k8::K8MetaItem;
use crate::stores::spu::SpuSpec;
use crate::stores::LocalStore;
use crate::stores::actions::WSAction;
use crate::cli::TlsConfig;

use super::spu_k8_config::ScK8Config;
use super::statefulset::StatefulsetSpec;
use super::spg_service::SpgServiceSpec;

#[derive(Debug)]
pub struct SpuGroupObj {
    inner: MetadataStoreObject<SpuGroupSpec, K8MetaItem>,
    svc_name: String,
}

impl Deref for SpuGroupObj {
    type Target = MetadataStoreObject<SpuGroupSpec, K8MetaItem>;

    fn deref(&self) -> &Self::Target {
        &self.inner
    }
}

impl SpuGroupObj {
    pub fn new(inner: MetadataStoreObject<SpuGroupSpec, K8MetaItem>) -> Self {
        let svc_name = format!("fluvio-spg-{}", inner.key());
        Self { inner, svc_name }
    }

    pub fn is_already_valid(&self) -> bool {
        self.status().is_already_valid()
    }

    pub async fn is_conflict_with(
        &self,
        spu_store: &LocalStore<SpuSpec, K8MetaItem>,
    ) -> Option<SpuId> {
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
    pub fn as_statefulset(
        &self,
        namespace: &str,
        spu_k8_config: &ScK8Config,
        tls: Option<&TlsConfig>,
    ) -> (String, WSAction<StatefulsetSpec, K8MetaItem>) {
        let statefulset_name = format!("fluvio-spg-{}", self.key());
        let k8_spec = k8_convert::generate_k8_stateful(
            &self.spec,
            self.key(),
            &self.svc_name,
            namespace,
            spu_k8_config,
            tls,
        );

        trace!(?k8_spec);

        (
            statefulset_name.clone(),
            WSAction::Apply(
                MetadataStoreObject::with_spec(statefulset_name, k8_spec.into())
                    .with_context(self.ctx().create_child()),
            ),
        )
    }

    /// generate as SPU spec
    #[instrument(skip(self))]
    pub fn as_spu(
        &self,
        spu: u16,
        services: &HashMap<String, IngressPort>,
    ) -> (String, MetadataStoreObject<SpuSpec, K8MetaItem>) {
        let spec = self.spec();
        let spu_id = compute_spu_id(spec.min_id, spu);
        let spu_name = format!("{}-{}", self.key(), spu);

        let spu_private_ep = SpuEndpointTemplate::default_private();

        let spu_public_ep = SpuEndpointTemplate::default_public();
        let public_endpoint = if let Some(ingress) = services.get(&spu_name) {
            debug!(%ingress);
            ingress.clone()
        } else {
            IngressPort {
                port: spu_public_ep.port,
                encryption: spu_public_ep.encryption.clone(),
                ingress: vec![],
            }
        };

        let ns = self.ctx().item().namespace();
        let private_svc_fqdn = format!(
            "fluvio-spg-main-{spu}.fluvio-spg-{}.{ns}.svc.cluster.local",
            self.key()
        );
        let public_svc_fqdn = format!("fluvio-spu-{spu_name}.{ns}.svc.cluster.local");

        let spu_spec = SpuSpec {
            id: spu_id,
            spu_type: SpuType::Managed,
            public_endpoint,
            private_endpoint: Endpoint {
                host: private_svc_fqdn,
                port: spu_private_ep.port,
                encryption: spu_private_ep.encryption,
            },
            rack: None,
            public_endpoint_local: Some(Endpoint {
                host: public_svc_fqdn,
                port: spu_public_ep.port,
                encryption: spu_public_ep.encryption,
            }),
        };

        /*
        // add spu as children of spg
        let mut ctx = spg_obj.ctx().create_child().set_labels(vec![(
            "fluvio.io/spu-group".to_string(),
            spg_obj.key().to_string(),
        )]);
        */

        (
            spu_name.clone(),
            MetadataStoreObject::with_spec(spu_name, spu_spec)
                .with_context(self.ctx().create_child()),
        )
    }

    pub fn as_service(&self) -> (String, WSAction<SpgServiceSpec, K8MetaItem>) {
        let svc_name = self.svc_name.to_owned();
        let k8_service = k8_convert::generate_service(self.spec(), self.key());

        (
            svc_name.clone(),
            WSAction::Apply(
                MetadataStoreObject::with_spec(svc_name, k8_service.into())
                    .with_context(self.ctx().create_child()),
            ),
        )
    }
}

/// compute spu id with min_id as base
fn compute_spu_id(min_id: i32, replica_index: u16) -> i32 {
    replica_index as i32 + min_id
}

mod k8_convert {

    use std::collections::HashMap;

    use fluvio_stream_model::k8_types::*;
    use fluvio_stream_model::k8_types::core::pod::{
        ContainerSpec, ContainerPortSpec, PodSpec, VolumeMount, VolumeSpec, SecretVolumeSpec,
    };
    use fluvio_stream_model::k8_types::core::service::*;
    use fluvio_stream_model::k8_types::app::stateful::{
        PersistentVolumeClaim, VolumeAccessMode, ResourceRequirements, VolumeRequest,
    };
    use fluvio_types::defaults::{
        SPU_DEFAULT_NAME, SPU_PUBLIC_PORT, SPU_PRIVATE_PORT, SC_PRIVATE_PORT, PRODUCT_NAME,
        TLS_SERVER_SECRET_NAME,
    };

    use crate::stores::spg::SpuGroupSpec;
    use super::super::statefulset::K8StatefulSetSpec;
    use super::{ScK8Config, TlsConfig};

    /// convert spu group spec into k8 statefulset spec
    pub fn generate_k8_stateful(
        spg_spec: &SpuGroupSpec,
        group_name: &str,
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

        let spu_pod_config = &spu_k8_config.spu_pod_config;

        let mut env = vec![
            Env::key_field_ref("SPU_INDEX", "metadata.name"),
            Env::key_value("SPU_MIN", &format!("{}", spg_spec.min_id)),
        ];

        // add RUST LOG, if passed
        if let Ok(rust_log) = std::env::var("RUST_LOG") {
            env.push(Env::key_value("RUST_LOG", &rust_log));
        }

        env.append(&mut spu_pod_config.extra_env.clone());

        let mut volume_mounts = vec![VolumeMount {
            name: "data".to_owned(),
            mount_path: format!("/var/lib/{PRODUCT_NAME}/data"),
            ..Default::default()
        }];

        let mut volumes = vec![];

        let mut args = vec![
            "/fluvio-run".to_owned(),
            "spu".to_owned(),
            "--sc-addr".to_owned(),
            format!("fluvio-sc-internal.{namespace}.svc.cluster.local:{SC_PRIVATE_PORT}"),
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
                    secret_name: tls
                        .secret_name
                        .clone()
                        .unwrap_or_else(|| TLS_SERVER_SECRET_NAME.to_string()),
                    ..Default::default()
                }),
                ..Default::default()
            });

            args.push("--bind-non-tls-public".to_owned());
            args.push("0.0.0.0:9007".to_owned());
        }

        volume_mounts.append(&mut spu_pod_config.extra_volume_mounts.clone());
        volumes.append(&mut spu_pod_config.extra_volumes.clone());

        let mut containers = vec![ContainerSpec {
            name: SPU_DEFAULT_NAME.to_owned(),
            image: Some(spu_k8_config.image.clone()),
            resources: spu_pod_config.resources.clone(),
            ports: vec![public_port, private_port],
            volume_mounts,
            env,
            args,
            ..Default::default()
        }];

        containers.append(&mut spu_pod_config.extra_containers.clone());

        let template = TemplateSpec {
            metadata: Some(
                TemplateMeta::default()
                    .set_labels(vec![("app", SPU_DEFAULT_NAME), ("group", group_name)]),
            ),
            spec: PodSpec {
                termination_grace_period_seconds: Some(10),
                containers,
                volumes,
                security_context: spu_k8_config.pod_security_context.clone(),
                node_selector: Some(spu_pod_config.node_selector.clone()),
                ..Default::default()
            },
        };
        let claim = PersistentVolumeClaim {
            access_modes: vec![VolumeAccessMode::ReadWriteOnce],
            storage_class_name: spu_pod_config.storage_class.clone(),
            resources: ResourceRequirements {
                requests: VolumeRequest { storage: size },
            },
        };

        K8StatefulSetSpec {
            replicas: Some(replicas),
            service_name: group_svc_name.to_owned(),
            selector: LabelSelector::new_labels(vec![
                ("app", SPU_DEFAULT_NAME),
                ("group", group_name),
            ]),
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
    pub fn generate_service(_spg: &SpuGroupSpec, group_name: &str) -> ServiceSpec {
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
        selector.insert("group".to_owned(), group_name.to_owned());

        ServiceSpec {
            cluster_ip: "None".to_owned(),
            ports: vec![public_port, private_port],
            selector: Some(selector),
            ..Default::default()
        }
    }
}

#[cfg(test)]
mod test {
    use std::collections::HashMap;

    use fluvio_sc_schema::{
        core::MetadataContext,
        spg::SpuGroupSpec,
        store::{k8::K8MetaItem, MetadataStoreObject},
    };

    use crate::k8::objects::spg_group::SpuGroupObj;

    #[test]
    fn test_as_spu_id_private_endpoint_for_k8s() {
        let spu_cases = vec![0, 1, 2];

        for spu in spu_cases {
            let mut item = K8MetaItem::default();
            "default".clone_into(&mut item.namespace);
            let ctx = MetadataContext::new(item);

            let inner: MetadataStoreObject<SpuGroupSpec, K8MetaItem> =
                MetadataStoreObject::new_with_context(
                    spu.to_string(),
                    SpuGroupSpec::default(),
                    ctx,
                );

            let spu_group = SpuGroupObj::new(inner);
            let services = HashMap::new();
            let as_spu = spu_group.as_spu(spu, &services);
            let private_endpoint = as_spu.1.spec.private_endpoint;

            assert_eq!(
                private_endpoint.host,
                format!("fluvio-spg-main-{spu}.fluvio-spg-{spu}.default.svc.cluster.local")
            );
            assert_eq!(private_endpoint.port, 9006);
        }
    }
}
