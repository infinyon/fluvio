use serde::Deserialize;
use serde::Serialize;

use k8_metadata::core::metadata::LabelSelector;
use k8_metadata::core::metadata::TemplateSpec;

use crate::pod::PodSpec;


use k8_metadata::core::Crd;
use k8_metadata::core::CrdNames;
use k8_metadata::core::Spec;
use k8_metadata::core::Status;


const STATEFUL_API: Crd = Crd {
    group: "apps",
    version: "v1",
    names: CrdNames {
        kind: "StatefulSet",
        plural: "statefulsets",
        singular: "statefulset",
    },
};

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StatefulSetSpec {
    pub pod_management_policy: Option<PodMangementPolicy>,
    pub replicas: Option<u16>,
    pub revision_history_limit: Option<u16>,
    pub selector: LabelSelector,
    pub service_name: String,
    pub template: TemplateSpec<PodSpec>,
    pub volume_claim_templates: Vec<TemplateSpec<PersistentVolumeClaim>>,
    pub update_strategy: Option<StatefulSetUpdateStrategy>
}

impl Spec for StatefulSetSpec {

    type Status = StatefulSetStatus;

    fn metadata() -> &'static Crd {
        &STATEFUL_API
    }
}


#[derive(Deserialize, Serialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StatefulSetUpdateStrategy {
    pub _type: String,
    pub rolling_ipdate: Option<RollingUpdateStatefulSetStrategy>

}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct RollingUpdateStatefulSetStrategy {
    partition: u32
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub enum PodMangementPolicy {
    OrderedReady,
    Parallel,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct PersistentVolumeClaim {
    pub access_modes: Vec<VolumeAccessMode>,
    pub storage_class_name: String,
    pub resources: ResourceRequirements,
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub enum VolumeAccessMode {
    ReadWriteOnce,
    ReadWrite,
    ReadOnlyMany,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct ResourceRequirements {
    pub requests: VolumeRequest,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
pub struct VolumeRequest {
    pub storage: String,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StatefulSetStatus {
    pub replicas: u16,
    pub collision_count: Option<u32>,
    pub conditions: Option<Vec<StatefulSetCondition>>,
    pub current_replicas: Option<u16>,
    pub current_revision: Option<String>,
    pub observed_generation: Option<u32>,
    pub ready_replicas: Option<u16>,
    pub update_revision: Option<String>,
    pub updated_replicas: Option<u16>,
}

impl Status for StatefulSetStatus{}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub enum StatusEnum {
    True,
    False,
    Unknown,
}

#[derive(Deserialize, Serialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct StatefulSetCondition {
    pub message: String,
    pub reason: StatusEnum,
    pub _type: String,
}


/*
#[cfg(test)]
mod test {

    use serde_json;
    use serde_json::json;

    use super::LabelSelector;
    use super::StatefulSetSpec;
    use k8_diff::Changes;
    use k8_metadata::cluster::ClusterSpec;
    use k8_metadata::cluster::Configuration;
    use k8_metadata::cluster::Cluster;
    use k8_metadata::cluster::ClusterEndpoint;

    #[test]
    fn test_label_selector() {
        let selector = LabelSelector::new_labels(vec![("app".to_owned(), "test".to_owned())]);

        let maps = selector.match_labels;
        assert_eq!(maps.len(), 1);
        assert_eq!(maps.get("app").unwrap(), "test");
    }

    #[test]
    fn test_cluster_to_stateful() {
        let cluster = ClusterSpec {
            cluster: Cluster {
                replicas: Some(3),
                rack: Some("rack1".to_string()),
                public_endpoint: Some(ClusterEndpoint::new(9005)),
                private_endpoint: Some(ClusterEndpoint::new(9006)),
                controller_endpoint: Some(ClusterEndpoint::new(9004)),
            },
            configuration: Some(Configuration::default()),
            env: None,
        };

        let stateful: StatefulSetSpec = (&cluster).into();
        assert_eq!(stateful.replicas, Some(3));
        let mut stateful2 = stateful.clone();
        stateful2.replicas = Some(2);

        let state1_json = serde_json::to_value(stateful).expect("json");
        let state2_json = serde_json::to_value(stateful2).expect("json");
        let diff = state1_json.diff(&state2_json).expect("diff");
        let json_diff = serde_json::to_value(diff).unwrap();
        assert_eq!(
            json_diff,
            json!({
                "replicas": 2
            })
        );
    }


    /*
    * TODO: make this as utility
    use std::io::Read;
    use std::fs::File;
    use k8_metadata::core::metadata::ObjectMeta;
    use k8_metadata::core::metadata::K8Obj;
    use super::StatefulSetStatus;
    use super::TemplateSpec;
    use super::PodSpec;
    use super::ContainerSpec;
    use super::ContainerPortSpec;

    #[test]
    fn test_decode_statefulset()  {
        let file_name = "/private/tmp/f1.json";

        let mut f = File::open(file_name).expect("open failed");
        let mut contents = String::new();
        f.read_to_string(&mut contents).expect("read file");
       // let st: StatefulSetSpec = serde_json::from_slice(&buffer).expect("error");
        let st: K8Obj<StatefulSetSpec,StatefulSetStatus> = serde_json::from_str(&contents).expect("error");
        println!("st: {:#?}",st);
        assert!(true);
    }
    */

}
*/