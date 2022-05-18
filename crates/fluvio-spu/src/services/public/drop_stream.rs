use std::io::Error as IoError;

use tracing::{instrument, trace, debug};

use dataplane::api::RequestMessage;
use fluvio_protocol::api::CloseSessionRequest;

use crate::core::DefaultSharedGlobalContext;

#[instrument(skip(req_msg, ctx))]
pub async fn handle_drop_stream_request(
    req_msg: RequestMessage<CloseSessionRequest>,
    ctx: DefaultSharedGlobalContext,
) -> Result<(), IoError> {
    let request = req_msg.request();
    let session_id = request.session_id;
    trace!(%session_id, "handling drop stream request");

    let publishers = ctx.stream_publishers();
    match publishers.remove_publisher(session_id).await {
        None => debug!(%session_id, "stream publisher not found"),
        Some(_) => trace!(%session_id, "stream publisher stopped"),
    }

    Ok(())
}
