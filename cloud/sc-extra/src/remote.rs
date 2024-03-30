use std::fmt::Debug;

use serde::{Serialize, Deserialize};

use fluvio_controlplane_metadata::core::Spec;
use fluvio_protocol::{Decoder, Encoder};
use fluvio_protocol::api::Request;

use super::CloudApiKey;
use super::CloudStatus;
use super::CloudRemoteClusterSpec;

pub enum RemoteCloudReqs {
    Register(RemoteRegister),
    Delete(RemoteDelete),
    List(RemoteList),
}

#[derive(Encoder, Decoder, Default, Debug, Clone)]
pub struct CloudRemoteClusterRequest<S> {
    pub request: S,
}

impl<S> CloudRemoteClusterSpec for CloudRemoteClusterRequest<S> where S: Encoder + Decoder {}

#[derive(Encoder, Decoder, Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemoteRegister {
    pub name: String,
    //pub rs_type: RemoteClusterType, // creates a circular dep
    pub rs_type: String,
}

impl CloudRemoteClusterSpec for RemoteRegister {}

impl Request for RemoteRegister {
    const API_KEY: u16 = CloudApiKey::Register as u16;
    type Response = CloudStatus;
}
impl Spec for RemoteRegister {
    const LABEL: &'static str = "CloudRemoteRegister";
    type IndexKey = String;
    type Status = CloudStatus;
    type Owner = Self;
}

impl Request for CloudRemoteClusterRequest<RemoteRegister> {
    const API_KEY: u16 = CloudApiKey::ReqRegister as u16;
    type Response = CloudStatus;
}

#[derive(Encoder, Decoder, Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RemoteList {}

impl CloudRemoteClusterSpec for RemoteList {}

impl Request for RemoteList {
    const API_KEY: u16 = CloudApiKey::List as u16;
    type Response = CloudStatus;
}
impl Spec for RemoteList {
    const LABEL: &'static str = "CloudRemoteList";
    type IndexKey = String;
    type Status = CloudStatus;
    type Owner = Self;
}

impl Request for CloudRemoteClusterRequest<RemoteList> {
    const API_KEY: u16 = CloudApiKey::ReqList as u16;
    type Response = CloudStatus;
}

#[derive(Encoder, Decoder, Default, Debug, Clone, PartialEq, Deserialize, Serialize)]
pub struct RemoteDelete {
    pub name: String,
}

impl CloudRemoteClusterSpec for RemoteDelete {}

impl Request for RemoteDelete {
    const API_KEY: u16 = CloudApiKey::Delete as u16;
    type Response = CloudStatus;
}

impl Spec for RemoteDelete {
    const LABEL: &'static str = "CloudRemoteDelete";
    type IndexKey = String;
    type Status = CloudStatus;
    type Owner = Self;
}

impl Request for CloudRemoteClusterRequest<RemoteDelete> {
    const API_KEY: u16 = CloudApiKey::ReqDelete as u16;
    type Response = CloudStatus;
}
