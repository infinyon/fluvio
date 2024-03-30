mod register;
mod unregister;
mod list;

pub use register::*;
pub use unregister::*;
pub use list::*;

// use anyhow::{anyhow, Result};
// use tracing::{info, instrument};

// use cloud_sc_extra::validate_req;
// use cloud_sc_extra::remote::RemoteCloudReqs;
// use cloud_sc_extra::{ListItem, CloudStatus};
// // use cloud_sc_extra::remote::CloudRemoteClusterRequest;
// use cloud_sc_extra::remote::*;

// use fluvio_auth::AuthContext;
// use fluvio_protocol::api::{RequestMessage, ResponseMessage};
// use fluvio_protocol::link::ErrorCode;
// use fluvio_stream_model::core::MetadataItem;
// use fluvio_sc_schema::cloud::ObjectCloudRequest;
// use fluvio_sc_schema::TryEncodableFrom;

// use crate::services::auth::AuthServiceContext;

// #[instrument(skip(request, auth_ctx))]
// pub async fn handle_cloud_request<AC: AuthContext, C: MetadataItem>(
//     request: RequestMessage<ObjectCloudRequest>,
//     auth_ctx: &AuthServiceContext<AC, C>,
// ) -> Result<ResponseMessage<CloudStatus>> {
//     info!("remote cluster register request {:?}", request);

//     let (header, req) = request.get_header_request();
//     let ctx = auth_ctx.global_ctx.clone();

//     let Ok(req) = try_convert_to_reqs(req) else {
//         return Ok(ResponseMessage::from_header(
//             &header,
//             CloudStatus::new(
//                 "decode error".to_owned(),
//                 ErrorCode::Other("unable to decode request".to_owned()),
//                 None,
//             ),
//         ));
//     };
//     match req {
//         RemoteCloudReqs::Register(req) => {
//             info!(name = req.name, "remote-cluster register");
//             if auth_ctx.global_ctx.config().read_only_metadata {
//                 info!(req=?req, "change requested in read-only config");
//                 return Ok(ResponseMessage::from_header(
//                     &header,
//                     CloudStatus::new(
//                         "read-only error".to_owned(),
//                         ErrorCode::Other("unable to change read-only configuration".to_owned()),
//                         None,
//                     ),
//                 ));
//             }
//             let spec = validate_req(&req.rs_type)?;
//             ctx.remote_clusters()
//                 .create_spec(req.name, spec)
//                 .await
//                 .map(|_| ())?;
//         }
//         RemoteCloudReqs::Delete(req) => {
//             info!(name = req.name, "remote-cluster delete");
//             if auth_ctx.global_ctx.config().read_only_metadata {
//                 info!(req=?req, "change requested in read-only config");
//                 return Ok(ResponseMessage::from_header(
//                     &header,
//                     CloudStatus::new(
//                         "read-only error".to_owned(),
//                         ErrorCode::Other("unable to change read-only configuration".to_owned()),
//                         None,
//                     ),
//                 ));
//             }
//             ctx.remote_clusters().delete(req.name).await.map(|_| ())?;
//         }
//         RemoteCloudReqs::List(_req) => {
//             info!("remote-cluster list");
//             let rcs: Vec<ListItem> = auth_ctx
//                 .global_ctx
//                 .remote_clusters()
//                 .store()
//                 .read()
//                 .await
//                 .values()
//                 .map(|item| {
//                     let name = item.key.clone();
//                     let remote_type = item.spec.remote_type.to_string();
//                     ListItem {
//                         name,
//                         remote_type,
//                         pairing: "-".into(),
//                         status: "-".into(),
//                         last: "-".into(),
//                     }
//                 })
//                 .collect();
//             let mut resp = CloudStatus::new_ok("ok");
//             resp.list = Some(rcs);
//             return Ok(ResponseMessage::from_header(&header, resp));
//         }
//     }

//     let resp = CloudStatus::new_ok("ok");
//     Ok(ResponseMessage::from_header(&header, resp))
// }

// pub fn try_convert_to_reqs(ob: ObjectCloudRequest) -> Result<RemoteCloudReqs> {
//     if let Some(req) = ob.downcast()? as Option<CloudRemoteClusterRequest<RemoteRegister>> {
//         return Ok(RemoteCloudReqs::Register(req.request));
//     } else if let Some(req) = ob.downcast()? as Option<CloudRemoteClusterRequest<RemoteDelete>> {
//         return Ok(RemoteCloudReqs::Delete(req.request));
//     } else if let Some(req) = ob.downcast()? as Option<CloudRemoteClusterRequest<RemoteList>> {
//         return Ok(RemoteCloudReqs::List(req.request));
//     }

//     Err(anyhow!("Invalid Cloud Request"))
// }

// pub fn validate_req(rs_type_str: &str) -> Result<RemoteClusterSpec> {
//     let remote_type = match rs_type_str {
//         "mirror-edge" => RemoteClusterType::MirrorEdge,
//         _ => {
//             return Err(anyhow!("bad remote cluster type"));
//         }
//     };

//     // key_pair todo
//     let key_pair = KeyPair {
//         public_key: "".into(),
//         private_key: "".into(),
//     };
//     let spec = RemoteClusterSpec {
//         remote_type,
//         key_pair,
//     };

//     Ok(spec)
// }
