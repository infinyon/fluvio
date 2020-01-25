use std::fmt::Debug;
use std::fmt::Display;

use log::debug;
use log::error;
use log::trace;
use futures::future::FutureExt;
use futures::stream::Stream;
use futures::stream::StreamExt;
use futures::stream::BoxStream;
use async_trait::async_trait;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json;
use serde_json::Value;
use http::Uri;
use http::status::StatusCode;
use http::header::ACCEPT;
use http::header::CONTENT_TYPE;
use http::header::AUTHORIZATION;
use http::header::HeaderValue;
use isahc::prelude::*;
use isahc::HttpClient;

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
use k8_metadata::client::PatchMergeType;
use k8_metadata::client::MetadataClient;
use k8_metadata::client::TokenStreamResult;

use k8_config::K8Config;

use crate::stream::BodyStream;
use crate::ClientError;
use crate::K8HttpClientBuilder;

// For error mapping: see: https://doc.rust-lang.org/nightly/core/convert/trait.From.html







/// K8 Cluster accessible thru API
#[derive(Debug)]
pub struct K8Client {
    client: HttpClient,
    host: String,
    token: Option<String>
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
        let token = helper.token();
        debug!("using k8 token: {:#?}",token);
        Ok(
            Self {
                client,
                host,
                token
            }
        )
    }


    fn hostname(&self) -> &str {
        &self.host
    }

    fn finish_request<B>(&self,request: &mut Request<B>) -> Result<(),ClientError>
        where B: Into<Body>
    {
        if let Some(ref token) = self.token {
            let full_token = format!("Bearer {}",token);
            request.headers_mut().insert(AUTHORIZATION,HeaderValue::from_str(&full_token)?);
        }
        Ok(())
    }

    /// handle request. this is async function
    async fn handle_request<B,T>(
        &self,
        mut request: Request<B>
    ) -> Result<T, ClientError>
    where
        T: DeserializeOwned,
        B: Into<Body>
    {
        self.finish_request(&mut request)?;
        
        let mut resp = self.client.send_async(request).await?;

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


    /// return stream of chunks, chunk is a bytes that are stream thru http channel
    fn stream_of_chunks<S>(&self,uri: Uri) -> impl Stream< Item = Vec<u8>> + '_
    where
        K8Watch<S, S::Status>: DeserializeOwned,
        S: Spec + Debug,
        S::Status: Debug,
    {
        debug!("streaming: {}", uri);


        let ft = async move {

            let mut request = match http::Request::get(uri).body(Body::empty()) {
                Ok(req) => req,
                Err(err) =>  {
                    error!("error uri err: {}",err);
                    return BodyStream::empty();
                }
            };

            if let Err(err) = self.finish_request(&mut request) {
                error!("error finish request: {}",err);
                return  BodyStream::empty()
            };

            match self.client.send_async(request).await {
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


    fn stream<S>(&self, uri: Uri) -> impl Stream<Item = TokenStreamResult<S,S::Status,ClientError>> + '_ 
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
                    error!("error raw stream {}", String::from_utf8_lossy(&chunk).to_string());
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

}




#[async_trait]
impl MetadataClient for K8Client {

    type MetadataClientError = ClientError;    

    /// retrieval a single item
    async fn retrieve_item<S,M>(
        &self,
        metadata: &M
    ) -> Result<K8Obj<S,S::Status>, ClientError>
    where
        K8Obj<S,S::Status>: DeserializeOwned,
        S: Spec,
        M: K8Meta<S> + Send + Sync
    {
        let uri = metadata.item_uri(self.hostname());
        debug!("retrieving item: {}", uri);

        self.handle_request(http::Request::get(uri).body(Body::empty())?).await
    }

    async fn retrieve_items<S>(
        &self,
        namespace: &str,
    ) -> Result<K8List<S,S::Status>, ClientError>
    where
        K8List<S,S::Status>: DeserializeOwned,
        S: Spec,
    {
        let uri = items_uri::<S>(self.hostname(), namespace, None);

        debug!("retrieving items: {}", uri);

        self.handle_request(http::Request::get(uri).body(Body::empty())?).await
    }

    async fn delete_item<S,M>(
        &self,
        metadata: &M,
    ) -> Result<K8Status, ClientError>
    where
        S: Spec,
        M: K8Meta<S> + Send + Sync
    {
        let uri = metadata.item_uri(self.hostname());
        debug!("delete item on url: {}", uri);

        self.handle_request(http::Request::delete(uri).body(Body::empty())?).await
    }

    /// create new object
    async fn create_item<S>(
        &self,
        value: InputK8Obj<S>
    ) -> Result<K8Obj<S,S::Status>, ClientError>
    where
        InputK8Obj<S>: Serialize + Debug,
        K8Obj<S,S::Status>: DeserializeOwned,
        S: Spec + Send,
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

        self.handle_request(request).await
    }

    /// update status
    async fn update_status<S>(
        &self,
        value: &UpdateK8ObjStatus<S,S::Status>,
    ) -> Result<K8Obj<S,S::Status>, ClientError>
    where
        UpdateK8ObjStatus<S,S::Status>: Serialize + Debug,
        K8Obj<S,S::Status>: DeserializeOwned,
        S: Spec + Send + Sync,
        S::Status: Send + Sync
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

        self.handle_request(request).await
    }

    /// patch existing with spec
    async fn patch_spec<S,M>(
        &self,
        metadata: &M,
        patch: &Value,
    ) -> Result<K8Obj<S,S::Status>, ClientError>
    where
        K8Obj<S,S::Status>: DeserializeOwned,
        S: Spec + Debug,
        M: K8Meta<S> + Display + Send + Sync
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

        self.handle_request(request).await
    }


    
    /// stream items since resource versions
    fn watch_stream_since<S>(
        &self,
        namespace: &str,
        resource_version: Option<String>,
    ) -> BoxStream<'_,TokenStreamResult<S,S::Status,Self::MetadataClientError>>
    where
        K8Watch<S,S::Status>: DeserializeOwned,
        S: Spec + Debug + 'static,
        S::Status: Debug
    {

        let opt = ListOptions {
            watch: Some(true),
            resource_version,
            timeout_seconds: Some(3600),
            ..Default::default()
        };
        let uri = items_uri::<S>(self.hostname(), namespace, Some(&opt));
        self.stream(uri).boxed()
    }
    
}
