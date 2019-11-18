use std::pin::Pin;
use std::marker::Unpin;
use std::task::Context;
use std::task::Poll;

use log::trace;
use futures::stream::Stream;
use futures::io::AsyncRead;
use isahc::Body;
use pin_utils::unsafe_pinned;



const BUFFER_SIZE: usize = 1000;

pub struct BodyStream {
    body: Body,
    done: bool
}


impl Unpin for BodyStream{}

impl BodyStream {
    
    unsafe_pinned!(body: Body);

    pub fn new(body: Body) -> Self {
        Self {
            body,
            done: false
        }
    }

    pub fn empty() -> Self {
        Self {
            body: Body::empty(),
            done: true
        }
    }
}


impl Stream for BodyStream {

    type Item = Vec<u8>;


    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context) -> Poll<Option<Self::Item>> {

        if self.done {
            return Poll::Ready(None);
        }

        // we maintain two buffer. outer is accumulate buffer
        // buf is temporary buffer.  this allow us to read unlimited size of chunks
        // after each polling, we move temp buffer into accumulated buffer
        let mut outer: Option<Vec<u8>> = None;
        let mut total_bytes: usize = 0;
        loop {
            let mut buf = vec![0;BUFFER_SIZE];
            match self.as_mut().body().poll_read(cx,&mut buf) {
                Poll::Pending => {
                    if total_bytes == 0 {
                        trace!("no prior read, pending, returning pending");
                        return Poll::Pending
                    } else {
                        trace!("encountered pending but prior bytes: {}, returning as next item",total_bytes);
                        return match outer {
                            Some(buf) => Poll::Ready(Some(buf)),
                            None => Poll::Pending           // should not reach here!!
                        }
                    }
                },
                Poll::Ready(read_result) => {
                    match read_result {
                        Err(err) => {
                            trace!("error reading: {}",err);
                            return Poll::Ready(None)
                        },
                        Ok(bytes_read) => {
                            buf.split_off(bytes_read);  // discard unread bytes
                            total_bytes += bytes_read;
                            trace!("bytes read: {}",bytes_read);
                            if bytes_read == 0 {
                                self.done = true;
                                if total_bytes == 0 {
                                    trace!("end reached but no accumulated bytes");
                                    return Poll::Ready(None)
                                } else {
                                    trace!("end reached,but we have accumulated bytes");
                                    trace!("total bytes accumulated: {}",total_bytes);
                                    return match outer {
                                        Some(outer_buf) => Poll::Ready(Some(outer_buf)),
                                        None => Poll::Ready(Some(buf))
                                    }
                                }
                            
                            } else {
                                // if bytes read exactly same as buffer size, then we may more bytes to read
                                if bytes_read  == BUFFER_SIZE {
                                    trace!("bytes read is same as buffer size,may have more bytes. looping");
                                    match outer.as_mut() {
                                        Some(outer_buf) => {
                                            outer_buf.append(&mut buf);
                                        },
                                        None => {
                                            outer.replace(buf);
                                        }
                                    }
                                } else {
                                    trace!("no more bytes, total bytes: {}",total_bytes);
                                    return match outer {
                                        Some(mut outer_buf) => {
                                            outer_buf.append(&mut buf);
                                            Poll::Ready(Some(outer_buf))
                                        },
                                        None => Poll::Ready(Some(buf))
                                    }
                                }
                                
                            }
                        }
                    }                    
                }
            }
        }
       
    }

}


