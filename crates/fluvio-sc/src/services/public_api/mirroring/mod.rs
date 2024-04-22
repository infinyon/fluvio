mod connect;

use std::sync::Arc;

use anyhow::{anyhow, Result};
use fluvio_controlplane_metadata::mirroring::{MirroringRemoteClusterRequest, MirrorConnect};
use fluvio_socket::ExclusiveFlvSink;
use tracing::{info, instrument};
use fluvio_auth::AuthContext;
use fluvio_protocol::api::RequestMessage;
use fluvio_stream_model::core::MetadataItem;
use fluvio_sc_schema::mirroring::ObjectMirroringRequest;
use fluvio_sc_schema::TryEncodableFrom;
use fluvio_types::event::StickyEvent;
use crate::services::auth::AuthServiceContext;
use crate::services::public_api::mirroring::connect::MirroringConnectController;

pub enum MirrorRequests {
    Connect(MirrorConnect),
}

#[instrument(skip(request, auth_ctx, sink, end_event))]
pub fn handle_mirroring_request<AC: AuthContext, C: MetadataItem>(
    request: RequestMessage<ObjectMirroringRequest>,
    auth_ctx: &AuthServiceContext<AC, C>,
    sink: ExclusiveFlvSink,
    end_event: Arc<StickyEvent>,
) -> Result<()> {
    info!("remote cluster register request {:?}", request);

    let (header, req) = request.get_header_request();
    let ctx = auth_ctx.global_ctx.clone();

    let Ok(req) = try_convert_to_reqs(req) else {
        return Err(anyhow!("unable to decode request"));
    };

    match req {
        MirrorRequests::Connect(req) => {
            MirroringConnectController::start(req, sink, end_event, ctx, header);
        }
    };

    Ok(())
}

pub fn try_convert_to_reqs(ob: ObjectMirroringRequest) -> Result<MirrorRequests> {
    if let Some(req) = ob.downcast()? as Option<MirroringRemoteClusterRequest<MirrorConnect>> {
        return Ok(MirrorRequests::Connect(req.request));
    }

    Err(anyhow!("Invalid Mirroring Request"))
}
