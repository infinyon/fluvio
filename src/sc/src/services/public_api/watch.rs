use std::sync::Arc;
use std::fmt::Debug;

use tracing::{debug, trace};
use tracing::error;
use tracing::instrument;

use fluvio_types::event::SimpleEvent;
use fluvio_socket::ExclusiveFlvSink;
use dataplane::core::{Encoder, Decoder};
use dataplane::api::{RequestMessage, RequestHeader, ResponseMessage};
use fluvio_sc_schema::objects::{WatchRequest, WatchResponse, Metadata, MetadataUpdate};

use fluvio_controlplane_metadata::core::Spec;
use fluvio_controlplane_metadata::partition::PartitionSpec;
use fluvio_controlplane_metadata::spu::SpuSpec;
use fluvio_controlplane_metadata::topic::TopicSpec;

use crate::services::auth::AuthServiceContext;
use crate::stores::{StoreContext, K8ChangeListener};
use fluvio_controlplane_metadata::spg::SpuGroupSpec;

/// handle watch request by spawning watch controller for each store
pub fn handle_watch_request<AC>(
    request: RequestMessage<WatchRequest>,
    auth_ctx: &AuthServiceContext<AC>,
    sink: ExclusiveFlvSink,
    end_event: Arc<SimpleEvent>,
) {
    debug!("handling watch request");
    let (header, req) = request.get_header_request();

    match req {
        WatchRequest::Topic(_) => WatchController::<TopicSpec>::update(
            sink,
            end_event,
            auth_ctx.global_ctx.topics().clone(),
            header,
        ),
        WatchRequest::Spu(_) => WatchController::<SpuSpec>::update(
            sink,
            end_event,
            auth_ctx.global_ctx.spus().clone(),
            header,
        ),
        WatchRequest::SpuGroup(_) => WatchController::<SpuGroupSpec>::update(
            sink,
            end_event,
            auth_ctx.global_ctx.spgs().clone(),
            header,
        ),
        WatchRequest::Partition(_) => WatchController::<PartitionSpec>::update(
            sink,
            end_event,
            auth_ctx.global_ctx.partitions().clone(),
            header,
        ),
    }
}

struct WatchController<S>
where
    S: Spec,
{
    response_sink: ExclusiveFlvSink,
    store: StoreContext<S>,
    header: RequestHeader,
    end_event: Arc<SimpleEvent>,
}

impl<S> WatchController<S>
where
    S: Spec + Debug + 'static + Send + Sync + Encoder + Decoder,
    S::IndexKey: ToString,
    <S as Spec>::Status: Sync + Send + Encoder + Decoder,
    <S as Spec>::IndexKey: Sync + Send,
    MetadataUpdate<S>: Into<WatchResponse>,
{
    /// start watch controller
    fn update(
        response_sink: ExclusiveFlvSink,
        end_event: Arc<SimpleEvent>,
        store: StoreContext<S>,
        header: RequestHeader,
    ) {
        use fluvio_future::task::spawn;

        let controller = Self {
            response_sink,
            store,
            end_event,
            header,
        };

        spawn(controller.dispatch_loop());
    }

    #[instrument(
        skip(self),
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
            MetadataUpdate::with_all(epoch, updates.into_iter().map(|u| u.into()).collect())
        } else {
            let (updates, deletes) = changes.parts();
            let mut changes: Vec<Message<Metadata<S>>> = updates
                .into_iter()
                .map(|v| Message::update(v.into()))
                .collect();
            let mut deletes = deletes
                .into_iter()
                .map(|d| Message::delete(d.into()))
                .collect();
            changes.append(&mut deletes);
            MetadataUpdate::with_changes(epoch, changes)
        };

        let resp_msg: ResponseMessage<WatchResponse> =
            ResponseMessage::from_header(&self.header, updates.into());

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
