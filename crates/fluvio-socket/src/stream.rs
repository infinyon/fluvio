use std::fmt;
use std::fmt::Debug;
use std::io::Cursor;
use std::io::Error as IoError;
use std::io::ErrorKind;

use fluvio_future::net::ConnectionFd;
use fluvio_future::net::{BoxReadConnection};
use fluvio_protocol::api::{ApiMessage, Request, RequestMessage, ResponseMessage};
use fluvio_protocol::codec::FluvioCodec;
use fluvio_protocol::Decoder as FluvioDecoder;
use futures_util::stream::{Stream, StreamExt};
use tokio_util::codec::{FramedRead};
use tokio_util::compat::FuturesAsyncReadCompatExt;
use tokio_util::compat::Compat;
use tracing::debug;
use tracing::error;
use tracing::trace;

use crate::SocketError;

type FrameStream = FramedRead<Compat<BoxReadConnection>, FluvioCodec>;

/// inner flv stream which is generic over stream
pub struct FluvioStream {
    inner: FrameStream,
    id: ConnectionFd,
}

impl Debug for FluvioStream {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "Stream({})", self.id)
    }
}

impl FluvioStream {
    pub fn new(id: ConnectionFd, stream: BoxReadConnection) -> Self {
        Self {
            inner: FramedRead::new(stream.compat(), FluvioCodec::new()),
            id,
        }
    }

    pub fn get_mut_tcp_stream(&mut self) -> &mut FrameStream {
        &mut self.inner
    }

    /// as server, get stream of request coming from client
    pub fn request_stream<R>(
        &mut self,
    ) -> impl Stream<Item = Result<RequestMessage<R>, SocketError>> + '_
    where
        RequestMessage<R>: FluvioDecoder + Debug,
    {
        (&mut self.inner).map(|req_bytes_r| match req_bytes_r {
            Ok(req_bytes) => {
                let mut src = Cursor::new(&req_bytes);
                let msg: RequestMessage<R> = RequestMessage::decode_from(&mut src, 0)?;
                Ok(msg)
            }
            Err(err) => Err(SocketError::Io {
                source: err,
                msg: "request stream".to_string(),
            }),
        })
    }

    /// as server, get next request from client
    pub async fn next_request_item<R>(&mut self) -> Option<Result<RequestMessage<R>, SocketError>>
    where
        RequestMessage<R>: FluvioDecoder + Debug,
    {
        let mut stream = self.request_stream();
        stream.next().await
    }

    /// as client, get next response from server
    pub async fn next_response<R>(
        &mut self,
        req_msg: &RequestMessage<R>,
    ) -> Result<ResponseMessage<R::Response>, SocketError>
    where
        R: Request,
    {
        trace!(api = R::API_KEY, "waiting for response");
        let next = self.inner.next().await;
        if let Some(result) = next {
            match result {
                Ok(req_bytes) => {
                    let response = req_msg.decode_response(
                        &mut Cursor::new(&req_bytes),
                        req_msg.header.api_version(),
                    )?;
                    trace!( len = req_bytes.len(), response = ?response,"received");
                    Ok(response)
                }
                Err(source) => {
                    error!("error receiving response: {:?}", source);
                    Err(SocketError::Io {
                        source,
                        msg: "next response".to_string(),
                    })
                }
            }
        } else {
            debug!("no more response. server has terminated connection");
            Err(IoError::new(ErrorKind::UnexpectedEof, "server has terminated connection").into())
        }
    }

    /// as server, get api request (PublicRequest, InternalRequest, etc)
    pub fn api_stream<R, A>(&mut self) -> impl Stream<Item = Result<R, SocketError>> + '_
    where
        R: ApiMessage<ApiKey = A>,
        A: FluvioDecoder + Debug,
    {
        (&mut self.inner).map(|req_bytes_r| match req_bytes_r {
            Ok(req_bytes) => {
                trace!("received bytes from client len: {}", req_bytes.len());
                let mut src = Cursor::new(&req_bytes);
                R::decode_from(&mut src).map_err(|err| err.into())
            }
            Err(err) => Err(SocketError::Io {
                source: err,
                msg: "api stream".to_string(),
            }),
        })
    }

    pub async fn next_api_item<R, A>(&mut self) -> Option<Result<R, SocketError>>
    where
        R: ApiMessage<ApiKey = A>,
        A: FluvioDecoder + Debug,
    {
        let mut stream = self.api_stream();
        stream.next().await
    }

    pub fn response_stream<R>(
        &mut self,
        req_msg: RequestMessage<R>,
    ) -> impl Stream<Item = R::Response> + '_
    where
        R: Request,
    {
        let version = req_msg.header.api_version();
        (&mut self.inner).filter_map(move |req_bytes| async move {
            match req_bytes {
                Ok(mut bytes) => match ResponseMessage::decode_from(&mut bytes, version) {
                    Ok(res_msg) => {
                        trace!("receive response: {:#?}", &res_msg);
                        Some(res_msg.response)
                    }
                    Err(err) => {
                        error!("error decoding response: {:?}", err);
                        None
                    }
                },
                Err(err) => {
                    error!("error receiving response: {:?}", err);
                    None
                }
            }
        })
    }
}

/*
impl From<FrameStream> for FluvioStream {
    fn from(stream: FrameStream) -> Self {
        FluvioStream {stream)
    }
}
*/
