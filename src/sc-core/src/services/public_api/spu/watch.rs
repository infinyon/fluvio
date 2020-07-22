use std::io::Error;

use log::{trace, debug};

use sc_api::objects::*;
use sc_api::spu::SpuSpec;
use sc_api::objects::*;
use flv_metadata::store::*;
use flv_future_aio::task::spawn;
use kf_socket::*;

use crate::core::SharedContext;


pub struct WatchController<S> {
    response_sink: InnerExclusiveKfSink<S>,
    context: SharedContext,
    metadata_request: WatchMetadataRequest,
    header: RequestHeader,
    end_event: Arc<Event>,
}

impl<S> ClientMetadataController<S>
where
    S: AsyncWrite + AsyncRead + Unpin + Send + ZeroCopyWrite + 'static,
{
    pub fn handle_metadata_update(
        request: RequestMessage<WatchMetadataRequest>,
        response_sink: InnerExclusiveKfSink<S>,
        end_event: Arc<Event>,
        context: SharedContext,
    ) {
        let (header, metadata_request) = request.get_header_request();
        let controller = Self {
            response_sink,
            context,
            header,
            metadata_request,
            end_event,
        };

        controller.run();
    }


async fn dispatch(epoch: Epoch,ctx: SharedContext) {

    let read_guard = ctx.spus().store().read().await;
    let changes = read_guard.changes_since(epoch);
    drop(read_guard);

    let epoch = changes.epoch;
    let is_sync_all = changes.is_sync_all();
    let (updates, deletes) = changes.parts();
    let request = if is_sync_all {
        UpdateSpuRequest::with_all(epoch, updates.into_iter().map(|u| u.spec).collect())
    } else {
        let mut changes: Vec<SpuMsg> = updates
            .into_iter()
            .map(|v| Message::update(v.spec))
            .collect();
        let mut deletes = deletes
            .into_iter()
            .map(|d| Message::delete(d.spec))
            .collect();
        changes.append(&mut deletes);
        UpdateSpuRequest::with_changes(epoch, changes)
    };

    let mut message = RequestMessage::new_request(request);
    message.get_mut_header().set_client_id("sc");

    debug!(
        "sending to spu: {}, all: {}, changes: {}",
        spu_id,
        message.request.all.len(),
        message.request.changes.len()
    );
    sink.send_request(&message).await?;
    Ok(epoch)
}
