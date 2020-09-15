use std::io::Cursor;
use std::fmt::Debug;
use std::io::Error as IoError;
use std::io::ErrorKind;

use tracing::trace;
use tracing::error;
use futures::io::{AsyncRead, AsyncWrite};
use futures::Stream;
use futures::stream::StreamExt;
use futures::stream::SplitStream;
use tokio_util::codec::Framed;
use tokio_util::compat::Compat;
use flv_future_aio::net::TcpStream;
use flv_future_aio::net::tls::AllTcpStream;
use dataplane_protocol::api::{ Request, RequestMessage, ResponseMessage, ApiMessage };
use fluvio_protocol::codec::FluvioCodec;
use dataplane_protocol::core::Decoder as FluvioDecoder;


use crate::KfSocketError;

pub type KfStream = InnerKfStream<TcpStream>;
#[allow(unused)]
pub type AllKfStream = InnerKfStream<AllTcpStream>;

type FrameStream<S> = SplitStream<Framed<Compat<S>, FluvioCodec>>;

/// inner kf stream which is generic over stream
#[derive(Debug)]
pub struct InnerKfStream<S>(FrameStream<S>);

impl<S> InnerKfStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    pub fn get_mut_tcp_stream(&mut self) -> &mut FrameStream<S> {
        &mut self.0
    }

    /// as server, get stream of request coming from client
    pub fn request_stream<R>(
        &mut self,
    ) -> impl Stream<Item = Result<RequestMessage<R>, KfSocketError>> + '_
    where
        RequestMessage<R>: FluvioDecoder + Debug,
    {
        (&mut self.0).map(|req_bytes_r| match req_bytes_r {
            Ok(req_bytes) => {
                let mut src = Cursor::new(&req_bytes);
                let msg: RequestMessage<R> = RequestMessage::decode_from(&mut src, 0)?;
                Ok(msg)
            }
            Err(err) => Err(err.into()),
        })
    }

    /// as server, get next request from client
    pub async fn next_request_item<R>(&mut self) -> Option<Result<RequestMessage<R>, KfSocketError>>
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
    ) -> Result<ResponseMessage<R::Response>, KfSocketError>
    where
        R: Request,
    {
        trace!("waiting for response");
        let next = self.0.next().await;
        if let Some(result) = next {
            match result {
                Ok(req_bytes) => {
                    let response = req_msg.decode_response(
                        &mut Cursor::new(&req_bytes),
                        req_msg.header.api_version(),
                    )?;
                    trace!("receive response: {:#?}", &response);
                    Ok(response)
                }
                Err(err) => {
                    error!("error receiving response: {:?}", err);
                    Err(KfSocketError::IoError(err))
                }
            }
        } else {
            error!("no more response. server has terminated connection");
            Err(KfSocketError::IoError(IoError::new(
                ErrorKind::UnexpectedEof,
                "server has terminated connection",
            )))
        }
    }

    /// as server, get api request (PublicRequest, InternalRequest, etc)
    pub fn api_stream<R, A>(&mut self) -> impl Stream<Item = Result<R, KfSocketError>> + '_
    where
        R: ApiMessage<ApiKey = A>,
        A: FluvioDecoder + Debug,
    {
        (&mut self.0).map(|req_bytes_r| match req_bytes_r {
            Ok(req_bytes) => {
                trace!("received bytes from client len: {}", req_bytes.len());
                let mut src = Cursor::new(&req_bytes);
                R::decode_from(&mut src).map_err(|err| err.into())
            }
            Err(err) => Err(err.into()),
        })
    }

    pub async fn next_api_item<R, A>(&mut self) -> Option<Result<R, KfSocketError>>
    where
        R: ApiMessage<ApiKey = A>,
        A: FluvioDecoder + Debug,
    {
        let mut stream = self.api_stream();
        stream.next().await
    }

    pub fn response_stream<'a, R>(
        &'a mut self,
        req_msg: RequestMessage<R>,
    ) -> impl Stream<Item = R::Response> + 'a
    where
        R: Request,
    {
        let version = req_msg.header.api_version();
        (&mut self.0).filter_map(move |req_bytes| async move {
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

impl<S> From<FrameStream<S>> for InnerKfStream<S> {
    fn from(stream: FrameStream<S>) -> Self {
        InnerKfStream(stream)
    }
}
