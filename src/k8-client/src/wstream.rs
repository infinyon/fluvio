
use std::pin::Pin;
use std::marker::Unpin;
use std::task::Context;
use std::task::Poll;

use bytes::Bytes;
use futures::stream::Stream;

use log::error;
use log::trace;
use pin_utils::unsafe_pinned;
use pin_utils::unsafe_unpinned;
use std::mem;

//type ChunkList = Vec<Result<Vec<u8>, HyperError>>;

pub struct WatchStream<S>
where
    S: Stream,
{
    stream: S,
    last_buffer: Vec<u8>,
    chunks: ChunkList,
}

impl <S>Unpin for WatchStream<S> where S: Stream {}

impl<S> WatchStream<S>
where
    S: Stream<Item = Result<Chunk, HyperError>>,
{
    unsafe_pinned!(stream: S);
    unsafe_unpinned!(last_buffer: Vec<u8>);
    unsafe_unpinned!(chunks: ChunkList);

    pub fn new(stream: S) -> Self {
        WatchStream {
            stream,
            last_buffer: Vec::new(),
            chunks: Vec::new(),
        }
    }
}

const SEPARATOR: u8 = b'\n';

impl<S> Stream for WatchStream<S>
where
    S: Stream<Item = Result<Chunk, HyperError>>,
{
    type Item = Result<ChunkList, HyperError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {
        let mut chunks = mem::replace(self.as_mut().chunks(), Vec::new());
        let last_buffer = mem::replace(self.as_mut().last_buffer(), Vec::new());
        let mut buf: Bytes = last_buffer.into();
        trace!(
            "entering poll next with prev buf: {}, chunk: {}",
            buf.len(),
            chunks.len()
        );

        loop {
            trace!(
                "polling chunk with prev buf len: {}, chunk: {}",
                buf.len(),
                chunks.len()
            );
            let poll_result = self.as_mut().stream().poll_next(cx);
            match poll_result {
                Poll::Pending => {
                    trace!(
                        "stream is pending. returning pending.  saving buf len: {}",
                        buf.len()
                    );
                    if buf.len() > 0 {
                        mem::replace(self.as_mut().last_buffer(), buf.to_vec());
                    }
                    if chunks.len() > 0 {
                        mem::replace(self.as_mut().chunks(), chunks);
                    }
                    return Poll::Pending;
                }
                Poll::Ready(next_item) => match next_item {
                    None => {
                        trace!("none from stream. ready to return items");
                        return Poll::Ready(None);
                    }
                    Some(chunk_result) => match chunk_result {
                        Ok(chunk) => {
                            trace!("chunk: {}", String::from_utf8_lossy(&chunk).to_string());
                            buf.extend_from_slice(&chunk);
                            trace!(
                                "parsing chunk with len: {} accum buffer: {}",
                                chunk.len(),
                                buf.len()
                            );
                            loop {
                                if let Some(i) = buf.iter().position(|&c| c == SEPARATOR) {
                                    trace!("found separator at: {}", i);
                                    let head = buf.slice(0, i);
                                    buf = buf.slice(i + 1, buf.len());

                                    chunks.push(Ok(head.to_vec()));
                                } else {
                                    trace!("no separator found");
                                    break;
                                }
                            }
                            if buf.len() > 0 {
                                trace!(
                                    "remainder chars count: {}. they will be added to accum buffer",
                                    buf.len()
                                );
                                trace!(
                                    "current buf: {}",
                                    String::from_utf8_lossy(&buf).to_string()
                                );
                            } else {
                                trace!(
                                    "end of loop buf is empty. returning {} chunks",
                                    chunks.len()
                                );
                                return Poll::Ready(Some(Ok(chunks)));
                            }
                        }
                        Err(err) => {
                            error!("error: {}", err);
                            return Poll::Ready(Some(Err(err.into())));
                        }
                    },
                },
            }
        }
    }
}
