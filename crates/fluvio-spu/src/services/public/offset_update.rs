use std::{io::Error as IoError};

use tracing::{debug, error, instrument};
use fluvio_spu_schema::server::update_offset::{
    OffsetUpdateStatus, UpdateOffsetsRequest, UpdateOffsetsResponse,
};
use fluvio_protocol::link::ErrorCode;
use fluvio_protocol::api::{ResponseMessage, RequestMessage};
use crate::services::public::conn_context::ConnectionContext;

#[instrument(skip(conn_ctx, request))]
pub(crate) async fn handle_offset_update(
    request: RequestMessage<UpdateOffsetsRequest>,
    conn_ctx: &mut ConnectionContext,
) -> Result<ResponseMessage<UpdateOffsetsResponse>, IoError> {
    debug!("received stream updates");
    let (header, updates) = request.get_header_request();
    let publishers = conn_ctx.stream_publishers();
    let mut status_list = vec![];

    for update in updates.offsets {
        let maybe_publisher = publishers.get_publisher(update.session_id).await;
        let status = match maybe_publisher {
            Some(publisher) => {
                debug!(
                    offset_update = update.offset,
                    session_id = update.session_id,
                    "published offsets"
                );
                publisher.offset_publisher.update(update.offset);
                OffsetUpdateStatus {
                    session_id: update.session_id,
                    error: ErrorCode::None,
                }
            }
            None => {
                error!(
                    "invalid offset {}, session_id:{}",
                    update.offset, update.session_id
                );
                OffsetUpdateStatus {
                    session_id: update.session_id,
                    error: ErrorCode::FetchSessionNotFoud,
                }
            }
        };
        status_list.push(status);
    }
    let response = UpdateOffsetsResponse {
        status: status_list,
    };
    Ok(RequestMessage::<UpdateOffsetsRequest>::response_with_header(&header, response))
}
