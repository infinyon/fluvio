use std::task::Poll;
use std::task::Context;
use std::pin::Pin;
use std::io::Error as IoError;

use futures::io::AsyncWrite;

use crate::client::*;
use crate::ClientError;


/// interface to producer
pub struct Producer {
    topic: String,
    partition: i32,
    #[allow(unused)]
    serial: SerialClient
}

impl Producer {

    pub fn new(serial: SerialClient, topic: &str,partition: i32) -> Self {
        Self {
            serial,
            topic: topic.to_owned(),
            partition
        }
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn partition(&self) -> i32 {
        self.partition
    }


    pub async fn send_record(&mut self, _record: Vec<u8>) -> Result<(), ClientError> {
        todo!()
    }

}


impl AsyncWrite for Producer{

    fn poll_write(self: Pin<&mut Self>, _cx: &mut Context<'_>, _buf: &[u8])
            -> Poll<Result<usize,IoError>> {
        todo!()
    }
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(),IoError>> {
        todo!()
    }
    fn poll_close(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Result<(),IoError>> {
        todo!()
    }

    
}