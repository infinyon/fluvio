use serde::Deserialize;
use serde::Serialize;
use std::collections::HashMap;

use k8_metadata::core::Crd;
use k8_metadata::core::CrdNames;
use k8_metadata::core::Spec;
use k8_metadata::core::Status;
use k8_metadata::spu::Endpoint;


const SERVICE_API: Crd = Crd {
    group: "core",
    version: "v1",
    names: CrdNames {
        kind: "Service",
        plural: "services",
        singular: "service",
    },
};

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ServiceSpec {
    #[serde(rename = "clusterIP")]
    pub cluster_ip: String,
    #[serde(rename = "externalIPs")]
    pub external_ips: Option<Vec<String>>,
    #[serde(rename = "loadBalancerIP")]
    pub load_balancer_ip: Option<String>,
    pub r#type: Option<LoadBalancerType>,
    pub external_name: Option<String>,
    pub external_traffic_policy: Option<ExternalTrafficPolicy>,
    pub ports: Vec<ServicePort>,
    pub selector: Option<HashMap<String, String>>,
}

impl Spec for ServiceSpec {

    type Status = ServiceStatus;

    fn metadata() -> &'static Crd {
        &SERVICE_API
    }


    fn make_same(&mut self,other: &Self)  {
        if other.cluster_ip == "" {
            self.cluster_ip = "".to_owned();
        }
    }

}


impl From<&Endpoint> for ServicePort {
    fn from(end_point: &Endpoint) -> Self {
        ServicePort {
            port: end_point.port,
            ..Default::default()
        }
    }
}



#[derive(Deserialize, Serialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ServicePort {
    pub name: Option<String>,
    pub node_port: Option<u16>,
    pub port: u16,
    pub target_port: Option<u16>,
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase",default)]
pub struct ServiceStatus {
    pub load_balancer: LoadBalancerStatus
}

impl Status for ServiceStatus{}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub enum ExternalTrafficPolicy {
    Local,
    Cluster
}

#[derive(Deserialize, Serialize, Debug, PartialEq, Clone)]
pub enum LoadBalancerType {
    ExternalName,
    ClusterIP,
    NodePort,
    LoadBalancer
}

#[derive(Deserialize, Serialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase",default)]
pub struct LoadBalancerStatus {
    pub ingress: Vec<LoadBalancerIngress>
}



#[derive(Deserialize, Serialize, Debug, Default, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LoadBalancerIngress {
    pub hostname: Option<String>,
    pub ip: Option<String>
}