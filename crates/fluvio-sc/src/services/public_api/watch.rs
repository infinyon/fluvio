use std::sync::Arc;
use std::io::Error as IoError;
use std::io::ErrorKind;

use fluvio_sc_schema::AdminSpec;
use tracing::{debug, trace, error, instrument};

use fluvio_types::event::StickyEvent;
use fluvio_socket::ExclusiveFlvSink;
use fluvio_protocol::{Encoder, Decoder};
use fluvio_protocol::api::{RequestMessage, RequestHeader, ResponseMessage};
use fluvio_sc_schema::objects::{
    ObjectApiWatchRequest, WatchResponse, Metadata, MetadataUpdate, ObjectApiWatchResponse,
};

use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_controlplane_metadata::spu::SpuSpec;
use fluvio_controlplane_metadata::topic::TopicSpec;
use fluvio_controlplane_metadata::smartmodule::SmartModuleSpec;
use fluvio_controlplane_metadata::tableformat::TableFormatSpec;

use crate::services::auth::AuthServiceContext;
use crate::stores::{StoreContext, K8ChangeListener};
use fluvio_controlplane_metadata::spg::SpuGroupSpec;

/// handle watch request by spawning watch controller for each store
#[instrument(skip(request, auth_ctx, sink, end_event))]
pub fn handle_watch_request<AC>(
    request: RequestMessage<ObjectApiWatchRequest>,
    auth_ctx: &AuthServiceContext<AC>,
    sink: ExclusiveFlvSink,
    end_event: Arc<StickyEvent>,
) -> Result<(), IoError> {
    let (header, req) = request.get_header_request();
    debug!("handling watch header: {:#?}, request: {:#?}", header, req);

    match req {
        ObjectApiWatchRequest::Topic(_) => WatchController::<TopicSpec>::update(
            sink,
            end_event,
            auth_ctx.global_ctx.topics().clone(),
            header,
            false,
        ),
        ObjectApiWatchRequest::Spu(_) => WatchController::<SpuSpec>::update(
            sink,
            end_event,
            auth_ctx.global_ctx.spus().clone(),
            header,
            false,
        ),
        ObjectApiWatchRequest::SpuGroup(_) => WatchController::<SpuGroupSpec>::update(
            sink,
            end_event,
            auth_ctx.global_ctx.spgs().clone(),
            header,
            false,
        ),
        ObjectApiWatchRequest::Partition(_) => WatchController::<PartitionSpec>::update(
            sink,
            end_event,
            auth_ctx.global_ctx.partitions().clone(),
            header,
            false,
        ),
        ObjectApiWatchRequest::SmartModule(sm_req) => WatchController::<SmartModuleSpec>::update(
            sink,
            end_event,
            auth_ctx.global_ctx.smartmodules().clone(),
            header,
            sm_req.summary,
        ),
        ObjectApiWatchRequest::TableFormat(_) => WatchController::<TableFormatSpec>::update(
            sink,
            end_event,
            auth_ctx.global_ctx.tableformats().clone(),
            header,
            false,
        ),
        _ => {
            debug!("Invalid Watch Req {:?}", req);
            return Err(IoError::new(
                ErrorKind::InvalidData,
                "Not Valid Watch Request",
            ));
        }
    }

    Ok(())
}

/// Watch controller for each object.  Note that return type may or not be the same as the object hence two separate spec
struct WatchController<S: AdminSpec> {
    response_sink: ExclusiveFlvSink,
    store: StoreContext<S>,
    header: RequestHeader,
    summary: bool,
    end_event: Arc<StickyEvent>,
}

impl<S> WatchController<S>
where
    S: AdminSpec + 'static,
    S: Encoder + Decoder + Send + Sync,
    S::Status: Encoder + Decoder + Send + Sync,
    S::IndexKey: ToString + Send + Sync,
    ObjectApiWatchResponse: From<WatchResponse<S>>,
{
    /// start watch controller
    fn update(
        response_sink: ExclusiveFlvSink,
        end_event: Arc<StickyEvent>,
        store: StoreContext<S>,
        header: RequestHeader,
        summary: bool,
    ) {
        use fluvio_future::task::spawn;

        let controller = Self {
            response_sink,
            store,
            header,
            end_event,
            summary,
        };

        spawn(controller.dispatch_loop());
    }

    #[instrument(
        skip(self),
        name = "WatchControllerLoop",
        fields(
            spec = S::LABEL,
            sink=self.response_sink.id()
        )
    )]
    async fn dispatch_loop(mut self) {
        use tokio::select;

        let mut change_listener = self.store.change_listener();

        loop {
            if !self.sync_and_send_changes(&mut change_listener).await {
                self.end_event.notify();
                break;
            }

            trace!("{}: waiting for changes", S::LABEL,);
            select! {

                _ = self.end_event.listen() => {
                    debug!("connection has been terminated");
                    break;
                },

                _ = change_listener.listen() => {
                    debug!("watch: {}, changes has been detected",S::LABEL);
                }
            }
        }

        debug!("watch: {} is done, terminating", S::LABEL);
    }

    /// sync with store and send out changes to send response
    /// if can't send, then signal end and return false
    #[instrument(skip(self, listener))]
    async fn sync_and_send_changes(&mut self, listener: &mut K8ChangeListener<S>) -> bool {
        use fluvio_controlplane_metadata::message::*;

        if !listener.has_change() {
            debug!("no changes, skipping");
        }

        let changes = listener.sync_changes().await;

        let epoch = changes.epoch;
        debug!(
            "watch: {} received changes with epoch: {},",
            S::LABEL,
            epoch
        );

        let updates = if changes.is_sync_all() {
            let (updates, _) = changes.parts();
            MetadataUpdate::with_all(
                epoch,
                updates
                    .into_iter()
                    .map(|u| u.into())
                    .map(|d: Metadata<S>| if self.summary { d.summary() } else { d })
                    .collect(),
            )
        } else {
            let (updates, deletes) = changes.parts();
            let mut changes: Vec<Message<Metadata<S>>> = updates
                .into_iter()
                .map(|u| u.into())
                .map(|d: Metadata<S>| if self.summary { d.summary() } else { d })
                .map(Message::update)
                .collect();
            let mut deletes = deletes
                .into_iter()
                .map(|d| Message::delete(d.into()))
                .collect();
            changes.append(&mut deletes);
            MetadataUpdate::with_changes(epoch, changes)
        };

        let response: WatchResponse<S> = WatchResponse::new(updates);
        let res: ObjectApiWatchResponse = response.into();
        let resp_msg = ResponseMessage::from_header(&self.header, res);

        // try to send response, if it fails then we need to end
        if let Err(err) = self
            .response_sink
            .send_response(&resp_msg, self.header.api_version())
            .await
        {
            error!(
                "error watch sending {}, correlation_id: {}, err: {}",
                S::LABEL,
                self.header.correlation_id(),
                err
            );
            // listen to other sender, that error has been occur, terminate their loop
            return false;
        }

        true
    }
}
