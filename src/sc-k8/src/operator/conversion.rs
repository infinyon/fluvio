/// convert spu group spec to statefulset for input
use std::collections::HashMap;

use k8_metadata::core::metadata::InputK8Obj;
use k8_metadata::core::metadata::InputObjectMeta;
use k8_metadata::core::metadata::Env;
use k8_metadata::core::metadata::ObjectMeta;
use k8_metadata::core::metadata::TemplateMeta;
use k8_metadata::core::metadata::LabelSelector;
use k8_metadata::core::metadata::TemplateSpec;
use k8_client::pod::ContainerSpec;
use k8_client::pod::ContainerPortSpec;
use k8_client::pod::PodSpec;
use k8_client::pod::VolumeMount;
use k8_client::stateful::ResourceRequirements;
use k8_client::stateful::VolumeRequest;
use k8_client::stateful::PersistentVolumeClaim;
use k8_client::stateful::StatefulSetSpec;
use k8_client::stateful::VolumeAccessMode;
use k8_client::service::ServiceSpec;
use k8_client::service::ServicePort;
use k8_metadata::core::metadata::LabelProvider;

use k8_metadata::spg::SpuGroupSpec;
use k8_metadata::core::Spec;
use types::defaults::SPU_DEFAULT_NAME;
use types::defaults::SPU_PUBLIC_PORT;
use types::defaults::SPU_PRIVATE_PORT;
use types::defaults::PRODUCT_NAME;
use types::defaults::IMAGE_NAME;
use types::defaults::FLV_LOG_BASE_DIR;
use types::defaults::FLV_LOG_SIZE;

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

/// convert SpuGroup to Statefulset
pub fn convert_cluster_to_statefulset(
        group_spec: &SpuGroupSpec,
        metadata: &ObjectMeta,
        group_name: &str,
        group_svc_name: String,
        namespace: &str) 
    -> InputK8Obj<StatefulSetSpec>
{

    let statefulset_name = format!("flv-spg-{}",group_name);
    let spec  = generate_stateful(group_spec, group_name,group_svc_name,namespace);
    let owner_ref = metadata.make_owner_reference::<SpuGroupSpec>();
    
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
    spg_spec: &SpuGroupSpec,
    name: &str,
    group_svc_name: String,
    namespace: &str) -> StatefulSetSpec {

    let replicas = spg_spec.replicas;
    let spg_template = &spg_spec.template.spec;
    let mut public_port = ContainerPortSpec {
        container_port: spg_template.public_endpoint.as_ref().map(|t| t.port).unwrap_or(SPU_PUBLIC_PORT),
        ..Default::default()
    };
    public_port.name = Some("public".to_owned());

    let mut private_port = ContainerPortSpec {
        container_port: spg_template.private_endpoint.as_ref().map(|t|t.port).unwrap_or(SPU_PRIVATE_PORT),
        ..Default::default()
    };
    private_port.name = Some("private".to_owned());

    // storage is special because defaults are explicit.
    let storage = spg_spec.template.spec.storage.clone().unwrap_or_default();
    let size =  storage.size();
    let mut env = vec![
        Env::key_field_ref("SPU_INDEX", "metadata.name"),
        Env::key_value("FLV_SC_PRIVATE_HOST",&format!("flv-sc-internal.{}.svc.cluster.local",namespace)),
        Env::key_value("SPU_MIN", &format!("{}",spg_spec.min_id())),
        Env::key_value(FLV_LOG_BASE_DIR,&storage.log_dir()),
        Env::key_value(FLV_LOG_SIZE, &size)
    ];
    
    env.append(&mut spg_template.env.clone());
    
    let template = TemplateSpec {
        metadata: Some(TemplateMeta::default().set_labels(
            vec![
                ("app", SPU_DEFAULT_NAME),
                ("group",name)
            ])),
        spec: PodSpec {
            termination_grace_period_seconds: Some(10),
            containers: vec![ContainerSpec {
                name: SPU_DEFAULT_NAME.to_owned(),
                image: Some(format!("{}:{}",IMAGE_NAME,VERSION)),
                ports: vec![public_port, private_port],
                volume_mounts: vec![VolumeMount {
                    name: "data".to_owned(),
                    mount_path: format!("/var/lib/{}/data", PRODUCT_NAME),
                    ..Default::default()
                }],
                env: Some(env),
                ..Default::default()
            }],
            ..Default::default()
        },
    };
    let claim = PersistentVolumeClaim {
        access_modes: vec![VolumeAccessMode::ReadWriteOnce],
        storage_class_name: format!("{}-{}", PRODUCT_NAME, SPU_DEFAULT_NAME),
        resources: ResourceRequirements {
            requests: VolumeRequest {
                storage: size
            },
        },
    };

    StatefulSetSpec {
        replicas: Some(replicas),
        service_name: group_svc_name,
        selector: LabelSelector::new_labels(vec![
            ("app", SPU_DEFAULT_NAME),
            ("group",name)
        ]),
        template,
        volume_claim_templates: vec![TemplateSpec {
            spec: claim,
            metadata: Some(TemplateMeta::named("data")),
        }],
        ..Default::default()
    }
}


pub fn generate_service(spg: &SpuGroupSpec,name: &str) -> ServiceSpec {

    let spg_template = &spg.template.spec;
    let mut public_port = ServicePort {
        port: spg_template.public_endpoint.as_ref().map(|t|t.port).unwrap_or(SPU_PUBLIC_PORT),
        ..Default::default()
    };

    public_port.name = Some("public".to_owned());
    let mut private_port = ServicePort {
        port: spg_template.private_endpoint.as_ref().map(|t|t.port).unwrap_or(SPU_PRIVATE_PORT),
        ..Default::default()
    };
    private_port.name = Some("private".to_owned());


    let mut selector = HashMap::new();
    selector.insert("app".to_owned(), SPU_DEFAULT_NAME.to_owned());
    selector.insert("group".to_owned(),name.to_owned());

    ServiceSpec {
        cluster_ip: "None".to_owned(),
        ports: vec![public_port, private_port],
        selector: Some(selector),
        ..Default::default()
    }
    
}