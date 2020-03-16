mod public_server;

mod flv;
mod kf;

mod api {
     // mixed
     pub use super::flv::api_versions_req::*;

     // kafka
     pub use super::kf::metadata_req::*;

     // fluvio
     pub use super::flv::create_topics_req::*;
     pub use super::flv::delete_topics_req::*;
     pub use super::flv::fetch_topics_req::*;
     pub use super::flv::topic_composition_req::*;

     pub use super::flv::create_custom_spus_req::*;
     pub use super::flv::delete_custom_spus_req::*;
     pub use super::flv::fetch_spu_req::*;

     pub use super::flv::create_spu_groups_req::*;
     pub use super::flv::delete_spu_groups_req::*;
     pub use super::flv::fetch_spu_groups_req::*;
}

use std::sync::Arc;
use std::fmt::Debug;

use log::info;
use log::debug;
use serde::Serialize;
use serde::de::DeserializeOwned;

use sc_api::PublicRequest;
use sc_api::ScApiKey;
use kf_service::KfApiServer;
use public_server::PublicService;
use k8_metadata::metadata::InputObjectMeta;
use k8_metadata::metadata::InputK8Obj;
use k8_metadata::metadata::K8List;
use k8_metadata::metadata::Spec as K8Spec;
use k8_metadata_client::MetadataClient;

use crate::core::ShareLocalStores;
use crate::metadata::K8WSUpdateService;
use crate::core::LocalStores;

pub type SharedPublicContext<C> = Arc<PublicContext<C>>;

pub type PublicApiServer<C> = KfApiServer<PublicRequest, ScApiKey, SharedPublicContext<C>, PublicService<C>>;

/// create public server
pub fn create_public_server<C>(
     metadata: ShareLocalStores,
     k8_ws: K8WSUpdateService<C>,
     namespace: String,
) -> PublicApiServer<C>
     where C: MetadataClient
{
     let addr = metadata.config().public_endpoint.addr.clone();
     info!("start public api service at: {}", addr);

     KfApiServer::new(
          addr,
          Arc::new(PublicContext {
               metadata,
               k8_ws,
               namespace,
          }),
          PublicService::new(),
     )
}

#[derive(Clone)]
pub struct PublicContext<C> {
     metadata: ShareLocalStores,
     k8_ws: K8WSUpdateService<C>,
     namespace: String,
}

impl <C>PublicContext<C> 
     where C: MetadataClient
{
     pub fn k8_client(&self) -> &C {
          self.k8_ws.client()
     }

     pub fn k8_ws(&self) -> &K8WSUpdateService<C> {
          &self.k8_ws
     }

     pub fn metadata(&self) -> &LocalStores {
          &self.metadata
     }

     /// Create input metadata for our context
     /// which has namespace
     pub async fn create<S>(
          &self,
          name: String,
          spec: S
     ) -> Result<(),C::MetadataClientError>
     where
          S: K8Spec + Serialize + Default + Debug + Clone + DeserializeOwned + Send,
          <S as K8Spec>::Status:  Default + Debug + Serialize + DeserializeOwned + Send
     {
          debug!("creating k8 spec: {:#?}",spec);
          let input = InputK8Obj {
               api_version: S::api_version(),
               kind: S::kind(),
               metadata: InputObjectMeta {
                    name,
                    namespace: self.namespace.clone(),
                    ..Default::default()
               },
               spec,
               ..Default::default()
          };

          let client = self.k8_ws.client();
          client.apply(input).await?;

          Ok(())
     }

     /// Create input metadata for our context
     /// which has namespace
     pub async fn delete<S>(
          &self,
          name: &str,
     ) -> Result<(),C::MetadataClientError>
     where
          S: K8Spec + Serialize + Default + Debug + Clone + DeserializeOwned ,
          <S as K8Spec>::Status:  Default + Debug + DeserializeOwned
     {
          debug!("deleting k8 obj: {}",name);
          let meta = InputObjectMeta {
               name: name.to_owned(),
               namespace: self.namespace.clone(),
               ..Default::default()
          };

          let client = self.k8_ws.client();
          client.delete_item::<S,_>(&meta).await?;

          Ok(())
     }

     /// retrieve all items in the namespace
     pub async fn retrieve_items<S>(
          &self
     ) -> Result<K8List<S>, C::MetadataClientError>
     where
          S: K8Spec,
           K8List<S>: DeserializeOwned,
     {
        
          let client = self.k8_ws.client();
          client.retrieve_items::<S,_>(self.namespace.clone()).await
     }


}
