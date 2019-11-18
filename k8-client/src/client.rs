use std::fmt::Debug;
use std::fmt::Display;

use futures::future::FutureExt;
use futures::stream::once;
use futures::future::ready;
use futures::stream::Stream;
use futures::stream::StreamExt;


use log::debug;
use log::error;
use log::trace;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json;
use serde_json::Value;
use http::Uri;
use http::status::StatusCode;
use http::header::ACCEPT;
use http::header::CONTENT_TYPE;
use isahc::prelude::*;
use isahc::ResponseFuture;
use isahc::HttpClient;

use k8_diff::Changes;
use k8_diff::Diff;
use k8_metadata::core::Crd;
use k8_metadata::core::Spec;
use k8_metadata::core::metadata::item_uri;
use k8_metadata::core::metadata::K8Meta;
use k8_metadata::core::metadata::items_uri;
use k8_metadata::core::metadata::InputK8Obj;
use k8_metadata::core::metadata::UpdateK8ObjStatus;
use k8_metadata::core::metadata::K8List;
use k8_metadata::core::metadata::K8Obj;
use k8_metadata::core::metadata::K8Status;
use k8_metadata::core::metadata::K8Watch;
use k8_metadata::core::options::ListOptions;
use k8_config::K8Config;

use crate::stream::BodyStream;
use crate::ClientError;
use crate::K8HttpClientBuilder;

// For error mapping: see: https://doc.rust-lang.org/nightly/core/convert/trait.From.html

pub type TokenStreamResult<S,P> = Result<Vec<Result<K8Watch<S,P>, ClientError>>, ClientError>;

pub fn as_token_stream_result<S>(events: Vec<K8Watch<S,S::Status>>) -> TokenStreamResult<S,S::Status> where S: Spec {

    Ok(events.into_iter().map(|event| Ok(event)).collect())
}

#[derive(Debug)]
pub enum ApplyResult<S,P>
{
    None,
    Created(K8Obj<S,P>),
    Patched(K8Obj<S,P>)
}

#[allow(dead_code)]
pub enum PatchMergeType {
    Json,
    JsonMerge,
    StrategicMerge, // for aggegration API
}

impl PatchMergeType {
    fn for_spec(crd: &Crd) -> Self {
        match crd.group {
            "core" => PatchMergeType::StrategicMerge,
            "apps" => PatchMergeType::StrategicMerge,
            _ => PatchMergeType::JsonMerge,
        }
    }

    fn content_type(&self) -> &'static str {
        match self {
            PatchMergeType::Json => "application/json-patch+json",
            PatchMergeType::JsonMerge => "application/merge-patch+json",
            PatchMergeType::StrategicMerge => "application/strategic-merge-patch+json",
        }
    }
}

/// used for comparing spec,
#[derive(Serialize, Debug, Clone)]
struct DiffSpec<S> {
    spec: S,
}

impl<S> DiffSpec<S>
where
    S: Serialize,
{
    fn from(spec: S) -> Self {
        DiffSpec { spec }
    }
}

/// K8 Cluster accessible thru API
#[derive(Debug)]
pub struct K8Client {
    client: HttpClient,
    host: String
}

impl K8Client {

    // load using default k8 config
    pub fn default() -> Result<Self,ClientError> {
        let config = K8Config::load()?;
        Self::new(config)
    }

    pub fn new(config: K8Config) -> Result<Self, ClientError> {

        
        let helper = K8HttpClientBuilder::new(config);
        let client = helper.build()?;
        let host = helper.config().api_path().to_owned();
        Ok(
            Self {
                client,
                host
            }
        )
    }

    
    /// handle request. this is async function
    async fn handle_request<T>(
        &self,
        response_future: ResponseFuture<'_>
    ) -> Result<T, ClientError>
    where
        T: DeserializeOwned,
    {
    
        let mut resp = response_future.await?;

        let status = resp.status();
        debug!("response status: {:#?}", status);

        if status == StatusCode::NOT_FOUND {
            return Err(ClientError::NotFound);
        }

        /*
        let text = resp.text()?;
        trace!("text: {}",text);
       
        serde_json::from_str(&text).map_err(|err| err.into())
        */
        resp.json().map_err(|err| err.into())
    }
    
    

    fn hostname(&self) -> &str {
        &self.host
    }

    /// retrieval a single item
    pub async fn retrieve_item<S,M>(
        &self,
        metadata: &M
    ) -> Result<K8Obj<S,S::Status>, ClientError>
    where
        K8Obj<S,S::Status>: DeserializeOwned,
        S: Spec,
        M: K8Meta<S>
    {
        let uri = metadata.item_uri(self.hostname());
        debug!("retrieving item: {}", uri);

        let req = self.client.get_async(uri);
        self.handle_request(req).await
    }

    pub async fn retrieve_items<S>(
        &self,
        namespace: &str,
    ) -> Result<K8List<S,S::Status>, ClientError>
    where
        K8List<S,S::Status>: DeserializeOwned,
        S: Spec,
    {
        let uri = items_uri::<S>(self.hostname(), namespace, None);

        debug!("retrieving items: {}", uri);

        let req = self.client.get_async(uri);
        self.handle_request(req).await
    }

    pub async fn delete_item<S,M>(
        &self,
        metadata: &M,
    ) -> Result<K8Status, ClientError>
    where
        S: Spec,
        M: K8Meta<S>
    {
        let uri = metadata.item_uri(self.hostname());
        debug!("delete item on url: {}", uri);

        let req = self.client.delete_async(uri);
        self.handle_request(req).await
    }

    /// create new object
    pub async fn create_item<S>(
        &self,
        value: InputK8Obj<S>
    ) -> Result<K8Obj<S,S::Status>, ClientError>
    where
        InputK8Obj<S>: Serialize + Debug,
        K8Obj<S,S::Status>: DeserializeOwned,
        S: Spec,
    {
        let uri = items_uri::<S>(self.hostname(), &value.metadata.namespace, None);
        debug!("creating '{}'", uri);
        trace!("creating RUST {:#?}", &value);

       
        let bytes = serde_json::to_vec(&value)?;

        trace!(
            "create raw: {}",
             String::from_utf8_lossy(&bytes).to_string()
        );

        let request = Request::post(uri)
            .header(CONTENT_TYPE,"application/json")
            .body(bytes)?;

        let req = self.client.send_async(request);

        self.handle_request(req).await
    }

    /// apply object, this is similar to ```kubectl apply```
    /// for now, this doesn't do any optimization
    /// if object doesn't exist, it will be created
    /// if object exist, it will be patched by using strategic merge diff
    pub async fn apply<S>(
        &self,
        value: InputK8Obj<S>,
    ) -> Result<ApplyResult<S,S::Status>, ClientError>
    where
        InputK8Obj<S>: Serialize + Debug,
        K8Obj<S,S::Status>: DeserializeOwned + Debug,
        S: Spec + Serialize + Debug + Clone,
    {
        debug!("applying '{}' changes", value.metadata.name);
        trace!("applying {:#?}", value);
       
        match self.retrieve_item(&value.metadata).await {
            Ok(item) => {
                let mut old_spec = item.spec;
                old_spec.make_same(&value.spec);
                // we don't care about status
                let new_spec = serde_json::to_value(DiffSpec::from(value.spec.clone()))?;
                let old_spec = serde_json::to_value(DiffSpec::from(old_spec))?;
                let diff = old_spec.diff(&new_spec)?;
                match diff {
                    Diff::None => {
                        debug!("no diff detected, doing nothing");
                        Ok(ApplyResult::None)
                    }
                    Diff::Patch(p) => {
                        let json_diff = serde_json::to_value(p)?;
                        debug!("detected diff: old vs. new spec");
                        trace!("new spec: {:#?}", &new_spec);
                        trace!("old spec: {:#?}", &old_spec);
                        trace!("new/old diff: {:#?}", json_diff);
                        let patch_result = self.patch_spec(&value.metadata, &json_diff).await?;
                        Ok(ApplyResult::Patched(patch_result))
                    }
                    _ => Err(ClientError::PatchError),
                }
            }
            Err(err) => match err {
                ClientError::NotFound => {
                    debug!("item '{}' not found, creating ...", value.metadata.name);
                    let created_item = self.create_item(value.into()).await?;
                    return Ok(ApplyResult::Created(created_item));
                }
                _ => return Err(err),
            },
        }
    }

    /// update status
    pub async fn update_status<S>(
        &self,
        value: &UpdateK8ObjStatus<S,S::Status>,
    ) -> Result<K8Obj<S,S::Status>, ClientError>
    where
        UpdateK8ObjStatus<S,S::Status>: Serialize + Debug,
        K8Obj<S,S::Status>: DeserializeOwned,
        S: Spec
    {
        let uri = item_uri::<S>(
            self.hostname(),
            &value.metadata.name,
            &value.metadata.namespace,
            Some("/status"),
        );
        debug!("updating '{}' status - uri: {}", value.metadata.name, uri);
        trace!("update: {:#?}", &value);

        
        let bytes = serde_json::to_vec(&value)?;
        trace!(
            "update raw: {}",
            String::from_utf8_lossy(&bytes).to_string()
        );

        let request = Request::put(uri)
            .header(CONTENT_TYPE,"application/json")
            .body(bytes)?;

        let req = self.client.send_async(request);

        self.handle_request(req).await
    }

    /// patch existing with spec
    pub async fn patch_spec<S,M>(
        &self,
        metadata: &M,
        patch: &Value,
    ) -> Result<K8Obj<S,S::Status>, ClientError>
    where
        K8Obj<S,S::Status>: DeserializeOwned,
        S: Spec + Debug,
        M: K8Meta<S> + Display
    {
        debug!("patching item at '{}'", metadata);
        trace!("patch json value: {:#?}", patch);
        let uri = metadata.item_uri(self.hostname());
        let merge_type = PatchMergeType::for_spec(S::metadata());

        
        let bytes = serde_json::to_vec(&patch)?;

        trace!("patch raw: {}", String::from_utf8_lossy(&bytes).to_string());

        let request = Request::patch(uri)
            .header(ACCEPT,"application/json")
            .header(
                CONTENT_TYPE,
                merge_type.content_type(),
            )
            .body(bytes)?;

        let req = self.client.send_async(request);
        self.handle_request(req).await
    }

    
    /// return stream of chunks, chunk is a bytes that are stream thru http channel
    fn stream_of_chunks<S>(&self, uri: Uri) -> impl Stream< Item = Vec<u8>> + '_
    where
        K8Watch<S, S::Status>: DeserializeOwned,
        S: Spec + Debug,
        S::Status: Debug,
    {
        debug!("streaming: {}", uri);
       
        let ft = async move {
            match self.client.get_async(uri).await {
                Ok(response) => {
                    trace!("res status: {}", response.status());
                    trace!("res header: {:#?}", response.headers());
                    BodyStream::new(response.into_body())
                },
                Err(err) => {
                    error!("error getting streaming: {}",err);
                    BodyStream::empty()
                }
            }
           
        };

        ft.flatten_stream()
    }


    
    fn stream<S>(&self, uri: Uri) -> impl Stream<Item = TokenStreamResult<S,S::Status>> + '_ 
    where
        K8Watch<S, S::Status>: DeserializeOwned,
        S: Spec + Debug + 'static,
        S::Status: Debug
    {
        
        self.stream_of_chunks(uri).map(|chunk| {   

            trace!("decoding raw stream : {}", String::from_utf8_lossy(&chunk).to_string());

            let result: Result<K8Watch<S, S::Status>, serde_json::Error> = 
                serde_json::from_slice(&chunk).map_err(|err| {
                    error!("parsing error: {}", err);
                    err
                });
            Ok(vec![match result {
                Ok(obj) => {
                    trace!("de serialized: {:#?}", obj);
                    Ok(obj)
                }
                Err(err) => Err(err.into()),
            }])
        })
    }
                            
    
    
    /// stream items since resource versions
    pub fn watch_stream_since<S>(
        &self,
        namespace: &str,
        resource_version: Option<String>,
    ) -> impl Stream<Item = TokenStreamResult<S,S::Status>> + '_
    where
        K8Watch<S,S::Status>: DeserializeOwned,
        S: Spec + Debug + 'static,
        S::Status: Debug,
    {

        let opt = ListOptions {
            watch: Some(true),
            resource_version,
            timeout_seconds: Some(3600),
            ..Default::default()
        };
        let uri = items_uri::<S>(self.hostname(), namespace, Some(&opt));
        self.stream(uri)
    }
    

    
    /// return all list of items and future changes as stream
    pub fn watch_stream_now<S>(
        &self,
        ns: String,
    ) -> impl Stream<Item = TokenStreamResult<S,S::Status>> + '_
    where
        K8Watch<S,S::Status>: DeserializeOwned,
        K8List<S,S::Status>: DeserializeOwned,
        S: Spec + Debug + 'static 
        ,
        S::Status: Debug,
    {
        
        let ft_stream = async move {
            let namespace = ns.as_ref();
            let items_ft = self.retrieve_items(namespace);
            let item_now_result = items_ft.await;

            match item_now_result {
                Ok(item_now_list) => {
                    let resource_version = item_now_list.metadata.resource_version;

                    let items_watch_stream =
                        self.watch_stream_since(namespace, Some(resource_version));

                    let items_list = item_now_list
                        .items
                        .into_iter()
                        .map(|item| Ok(K8Watch::ADDED(item)))
                        .collect();
                    let list_stream = once(ready(Ok(items_list)));

                    list_stream.chain(items_watch_stream).left_stream()
                    // list_stream
                }
                Err(err) => once(ready(Err(err))).right_stream(),
            }
        };

        ft_stream.flatten_stream()

    }
    

    /// Check if the object exists, return true or false.
    pub async fn exists<S,M>(
        &self,
        metadata: &M,
    ) -> Result<bool, ClientError>
    where
        K8Obj<S,S::Status>: DeserializeOwned + Serialize + Debug + Clone,
        S: Spec + Serialize + Debug,
        M: K8Meta<S> + Display
    {
        debug!("check if '{}' exists", metadata);
        
        match self.retrieve_item(metadata).await {
            Ok(_) => Ok(true),
            Err(err) => match err {
                ClientError::NotFound => Ok(false),
                _ => Err(err),
            },
        }
    }
}
