use tracing::debug;

use dataplane::api::RequestMessage;
use fluvio_socket::FlvSocket;
use fluvio_socket::FlvSocketError;

use crate::core::DefaultSharedGlobalContext;
use crate::replication::leader::LeaderConnection;
use super::FetchStreamRequest;
use super::FetchStreamResponse;

pub(crate) async fn handle_fetch_stream_request(
    req_msg: RequestMessage<FetchStreamRequest>,
    ctx: DefaultSharedGlobalContext,
    mut socket: FlvSocket,
) -> Result<(), FlvSocketError> {
    let request = &req_msg.request;
    let follower_id = request.spu_id;
    debug!(
        "internal service: respond to fetch stream request, follower: {}",
        follower_id
    );

    let response = FetchStreamResponse::new(follower_id);
    let res_msg = req_msg.new_response(response);
    socket
        .get_mut_sink()
        .send_response(&res_msg, req_msg.header.api_version())
        .await?;

    LeaderConnection::handle(ctx, follower_id, socket).await?;

    Ok(()) as Result<(), FlvSocketError>
}
