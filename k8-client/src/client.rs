use std::fmt::Debug;
use std::fmt::Display;

use futures::compat::Future01CompatExt;
use futures::compat::Stream01CompatExt;
use futures::future::ready;
use futures::future::FutureExt;
use futures::stream::once;
use futures::stream::Stream;
use futures::stream::StreamExt;
use hyper;
use hyper::client::connect::HttpConnector;
use hyper::header::HeaderValue;
use hyper::header::ACCEPT;
use hyper::header::CONTENT_TYPE;
use hyper::http::request::Builder;
use hyper::rt::Stream as Stream01;
use hyper::Body;
use hyper::Client;
use hyper::Method;
use hyper::Request;
use hyper::StatusCode;
use hyper::Uri;
use log::debug;
use log::error;
use log::trace;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json;
use serde_json::Value;
use hyper_rustls::HttpsConnector;

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

use crate::wstream::WatchStream;
use crate::ClientError;
use crate::K8AuthHelper;

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
    client: Client<HttpsConnector<HttpConnector>>,
    auth_helper: K8AuthHelper,
    host: String
}

impl K8Client {
    pub fn new(config: K8Config) -> Result<Self, ClientError> {

        let helper = K8AuthHelper::new(config);
        match helper.build_https_connector()  {
            Ok(https) => {
                let hyper_client = Client::builder().build::<_, (Body)>(https);
                let host = helper.config.api_path().to_owned();
                
                Ok(Self {
                    auth_helper: helper,
                    client: hyper_client,
                    host
                })
            }
            Err(err) => {
                error!("error getting k8 client: {}", err);
                std::process::exit(-1);
            }
        }
    }

    /// handle request. this is async function
    async fn handle_request<T>(
        &self,
        req_result: Result<Request<Body>, ClientError>,
    ) -> Result<T, ClientError>
    where
        T: DeserializeOwned,
    {
        let req = req_result?;

        let resp = self.client.request(req).compat().await?;
        let status = resp.status();
        debug!("response status: {:#?}", status);

        if status == StatusCode::NOT_FOUND {
            return Err(ClientError::NotFound);
        }

        let body = resp.into_body().concat2().compat().await?;
        serde_json::from_slice(&body).map_err(|err| {
            error!("parser error: {}", err);
            let v = body.to_vec();
            let raw = String::from_utf8_lossy(&v).to_string();
            error!("raw: {}", raw);
            let v: serde_json::Value = serde_json::from_slice(&body).expect("this shoud parse");
            trace!("json struct: {:#?}", v);
            err.into()
        })
    }

    // build default request for uri
    fn default_req(&self, uri: Uri) -> Builder {
        let mut builder = Request::builder();
        builder.uri(uri);

        match &self.auth_helper.config {
            K8Config::Pod(pod_config) => {
                trace!("setting bearer token from pod config");
                builder.header("Authorization",pod_config.token.to_owned());
            },
            _ => {}
        }
            
        builder
    }

    /// return stream based on uri
    fn stream<S>(&self, uri: Uri) -> impl Stream<Item = TokenStreamResult<S, S::Status>>
    where
        K8Watch<S, S::Status>: DeserializeOwned,
        S: Spec + Debug,
        S::Status: Debug,
    {
        debug!("streaming: {}", uri);
        let req = self
            .default_req(uri)
            .method(Method::GET)
            .body(Body::empty())
            .unwrap();

        let req_ft = self.client.request(req).compat();

        let ft = async move {
            let resp = req_ft.await.unwrap();
            trace!("res status: {}", resp.status());
            trace!("res header: {:#?}", resp.headers());
            resp.into_body().compat()
        };

        WatchStream::new(ft.flatten_stream()).map(|chunks_result| {
            chunks_result
                .map(|chunk_list| {
                    chunk_list
                        .into_iter()
                        .map(|chunk_result| match chunk_result {
                            Ok(chunk) => {
                                let slice = chunk.as_slice();
                                let result: Result<K8Watch<S, S::Status>, serde_json::Error> =
                                    serde_json::from_slice(slice).map_err(|err| {
                                        error!("parsing error: {}", err);
                                        error!(
                                            "line: {}",
                                            String::from_utf8_lossy(slice).to_string()
                                        );
                                        err
                                    });
                                match result {
                                    Ok(obj) => {
                                        trace!("deserialized: {:#?}", obj);
                                        Ok(obj)
                                    }
                                    Err(err) => Err(err.into()),
                                }
                            }
                            Err(err) => Err(err.into()),
                        })
                        .collect()
                })
                .map_err(|err| err.into())
        })
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

        let req = self
            .default_req(uri)
            .method(Method::GET)
            .body(Body::empty())
            .map_err(|e| e.into());

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

        let req = self
            .default_req(uri)
            .method(Method::GET)
            .body(Body::empty())
            .map_err(|e| e.into());

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
        debug!("delete item wiht url: {}", uri);

        let req = self
            .default_req(uri)
            .method(Method::DELETE)
            .body(Body::empty())
            .map_err(|e| e.into());

        self.handle_request(req).await
    }

    pub async fn create_item<S,P>(
        &self,
        value: InputK8Obj<S>,
    ) -> Result<K8Obj<S,P>, ClientError>
    where
        InputK8Obj<S>: Serialize + Debug,
        K8Obj<S,P>: DeserializeOwned,
        S: Spec,
    {
        let uri = items_uri::<S>(self.hostname(), &value.metadata.namespace, None);
        debug!("creating '{}'", uri);
        trace!("creating RUST {:#?}", &value);

        let req = || -> Result<_, ClientError> {
            let bytes = serde_json::to_vec(&value)?;

            trace!(
                "create raw: {}",
                String::from_utf8_lossy(&bytes).to_string()
            );

            self.default_req(uri)
                .method(Method::POST)
                .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
                .body(Body::from(bytes))
                .map_err(|e| e.into())
        }();

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

        let req = || -> Result<_, ClientError> {
            let bytes = serde_json::to_vec(&value)?;
            trace!(
                "update raw: {}",
                String::from_utf8_lossy(&bytes).to_string()
            );
            self.default_req(uri)
                .method(Method::PUT)
                .header(CONTENT_TYPE, HeaderValue::from_static("application/json"))
                .body(Body::from(bytes))
                .map_err(|e| e.into())
        }();

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

        let req = || -> Result<_, ClientError> {
            let bytes = serde_json::to_vec(&patch)?;

            trace!("patch raw: {}", String::from_utf8_lossy(&bytes).to_string());

            self.default_req(uri)
                .method(Method::PATCH)
                .header(ACCEPT, HeaderValue::from_static("application/json"))
                .header(
                    CONTENT_TYPE,
                    HeaderValue::from_static(merge_type.content_type()),
                )
                .body(Body::from(bytes))
                .map_err(|e| e.into())
        }();

        self.handle_request(req).await
    }

    /// stream items since resource versions
    pub fn watch_stream_since<S>(
        &self,
        namespace: &str,
        resource_version: Option<String>,
    ) -> impl Stream<Item = TokenStreamResult<S,S::Status>>
    where
        K8Watch<S,S::Status>: DeserializeOwned,
        S: Spec + Debug,
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
        S: Spec + Debug,
        S::Status: Debug,
    {
        // future
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
