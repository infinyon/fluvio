use std::sync::Arc;
use std::{io::Error as IoError};

use tracing::{debug, error, instrument};
use fluvio_spu_schema::server::update_offset::{
    OffsetUpdateStatus, UpdateOffsetsRequest, UpdateOffsetsResponse,
};
use dataplane::ErrorCode;
use dataplane::api::{ResponseMessage, RequestMessage};
use crate::core::GlobalContext;

#[instrument(skip(ctx, request))]
pub async fn handle_offset_update(
    ctx: Arc<GlobalContext>,
    request: RequestMessage<UpdateOffsetsRequest>,
) -> Result<ResponseMessage<UpdateOffsetsResponse>, IoError> {
    debug!("received stream updates");
    let (header, updates) = request.get_header_request();
    let publishers = ctx.stream_publishers();
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
                publisher.update(update.offset);
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
