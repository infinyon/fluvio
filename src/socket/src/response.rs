
use core::task::{Context, Poll};
use std::io::Cursor;
use std::marker::PhantomData;
use std::pin::Pin;

use async_channel::Receiver;
use bytes::BytesMut;
use futures_util::stream::Stream;
use pin_project::{pin_project, pinned_drop};
use tracing::{debug, instrument, trace};

use fluvio_protocol::api::Request;
use fluvio_protocol::api::RequestHeader;
use fluvio_protocol::Decoder;

use crate::FlvSocketError;

/// Implement async socket where response are send back async manner
/// they are queued using channel
#[pin_project(PinnedDrop)]
pub struct AsyncResponse<R> {
    #[pin]
    pub(crate)receiver: Receiver<Option<BytesMut>>,
    pub(crate)header: RequestHeader,
    pub(crate)correlation_id: i32,
    pub(crate)data: PhantomData<R>,
}

#[pinned_drop]
impl<R> PinnedDrop for AsyncResponse<R> {
    fn drop(self: Pin<&mut Self>) {
        self.receiver.close();
        debug!("multiplexer stream: {} closed", self.correlation_id);
    }
}

impl<R: Request> Stream for AsyncResponse<R> {
    type Item = Result<R::Response, FlvSocketError>;

    #[instrument(
        skip(self, cx),
        fields(
            api_key = R::API_KEY,
            correlation_id = self.correlation_id,
        )
    )]
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        let next: Option<Option<_>> = match this.receiver.poll_next(cx) {
            Poll::Pending => {
                trace!("Waiting for async response");
                return Poll::Pending;
            }
            Poll::Ready(next) => next,
        };

        let bytes = if let Some(bytes) = next {
            bytes
        } else {
            return Poll::Ready(None);
        };

        let bytes = if let Some(bytes) = bytes {
            bytes
        } else {
            return Poll::Ready(Some(Err(FlvSocketError::SocketClosed)));
        };

        let mut cursor = Cursor::new(&bytes);
        let response = R::Response::decode_from(&mut cursor, this.header.api_version());

        let value = match response {
            Ok(value) => {
                trace!("Received response bytes: {},  {:#?}", bytes.len(), &value,);
                Some(Ok(value))
            }
            Err(e) => Some(Err(e.into())),
        };
        Poll::Ready(value)
    }
}
