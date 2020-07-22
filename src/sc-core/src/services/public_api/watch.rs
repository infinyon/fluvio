use std::sync::Arc;
use std::fmt::Debug;

use log::debug;
use log::error;
use event_listener::Event;
use futures::io::AsyncRead;
use futures::io::AsyncWrite;

use kf_socket::InnerExclusiveKfSink;
use kf_protocol::api::RequestMessage;
use kf_protocol::api::RequestHeader;
use kf_protocol::api::ResponseMessage;
use sc_api::objects::WatchRequest;
use sc_api::objects::WatchResponse;
use sc_api::objects::Metadata;
use sc_api::objects::MetadataUpdate;
use flv_future_aio::zero_copy::ZeroCopyWrite;
use flv_metadata::core::Spec;
use flv_metadata::store::Epoch;
use flv_metadata::partition::PartitionSpec;
use flv_metadata::spu::SpuSpec;

use crate::core::SharedContext;
use crate::stores::StoreContext;

/// handle watch request by spawning watch controller for each store
pub fn handle_watch_request<T>(
    request: RequestMessage<WatchRequest>,
    ctx: SharedContext,
    sink: InnerExclusiveKfSink<T>,
    end_event: Arc<Event>,
) where
    T: AsyncWrite + AsyncRead + Unpin + Send + ZeroCopyWrite + 'static,
{
    debug!("handling watch request");
    let (header, req) = request.get_header_request();

    match req {
        WatchRequest::Topic(_) => unimplemented!(),
        WatchRequest::Spu(epoch) => {
            WatchController::<T,SpuSpec>::update(epoch, sink, end_event, ctx.spus().clone(), header)
        }
        WatchRequest::SpuGroup(_) => unimplemented!(),
        WatchRequest::Partition(epoch) => {
            WatchController::<T,PartitionSpec>::update(epoch, sink, end_event, ctx.partitions().clone(), header)
        }
    }
}

struct WatchController<T, S>
where
    S: Spec,
{
    response_sink: InnerExclusiveKfSink<T>,
    store: StoreContext<S>,
    epoch: Epoch,
    header: RequestHeader,
    end_event: Arc<Event>,
}

impl<T, S> WatchController<T, S>
where
    T: AsyncWrite + AsyncRead + Unpin + Send + ZeroCopyWrite + 'static,
    S: Spec + Debug + 'static + Send + Sync,
    S::IndexKey: ToString,
    <S as Spec>::Status: Sync + Send,
    <S as Spec>::IndexKey: Sync + Send,
    MetadataUpdate<S>: Into<WatchResponse>,
{
    /// start watch controller
    fn update(
        epoch: Epoch,
        response_sink: InnerExclusiveKfSink<T>,
        end_event: Arc<Event>,
        store: StoreContext<S>,
        header: RequestHeader,
    ) {
        use flv_future_aio::task::spawn;

        let controller = Self {
            response_sink,
            store,
            epoch,
            end_event,
            header,
        };

        spawn(controller.dispatch_loop());
    }

    async fn dispatch_loop(mut self) {
        use tokio::select;

        // do initial sync
        if !self.sync_and_send_changes().await {
            debug!(
                "watch: {}, problem with initial sync, terminating",
                S::LABEL
            );
            return;
        }
        loop {
            debug!(
                "watch: {}, waiting for changes with epoch: {}",
                S::LABEL,
                self.epoch
            );
            select! {

                _ = self.end_event.listen() => {
                    debug!("watch: {}, connection has been terminated, terminating",S::LABEL);
                    break;
                },

                _ = self.store.listen() => {
                    debug!("watch: {}, changes in been detected",S::LABEL);

                    if !self.sync_and_send_changes().await {
                        debug!("watch: {}, problem with sync, terminating", S::LABEL);
                        break;
                    }
                }

            }
        }

        debug!("watch: {} is done, terminating", S::LABEL);
    }

    /// sync with store and send out changes to send response
    /// if can't send, then signal end and return false
    async fn sync_and_send_changes(&mut self) -> bool {
        use flv_metadata::message::*;

        let read_guard = self.store.store().read().await;
        let changes = read_guard.changes_since(self.epoch);
        drop(read_guard);

        debug!(
            "watch: {} received changes with epoch: {},",
            S::LABEL,
            changes.epoch
        );
        self.epoch = changes.epoch;

        let is_sync_all = changes.is_sync_all();
        let (updates, deletes) = changes.parts();
        let updates = if is_sync_all {
            MetadataUpdate::with_all(self.epoch, updates.into_iter().map(|u| u.into()).collect())
        } else {
            let mut changes: Vec<Message<Metadata<S>>> = updates
                .into_iter()
                .map(|v| Message::update(v.into()))
                .collect();
            let mut deletes = deletes
                .into_iter()
                .map(|d| Message::delete(d.into()))
                .collect();
            changes.append(&mut deletes);
            MetadataUpdate::with_changes(self.epoch, changes)
        };

        let resp_msg: ResponseMessage<WatchResponse> =
            ResponseMessage::from_header(&self.header, updates.into());

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
            self.end_event.notify(usize::MAX);
            return false;
        }

        return true;
    }
}
