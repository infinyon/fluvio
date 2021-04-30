cfg_if::cfg_if! {
    if #[cfg(unix)] {
        mod error;
        mod multiplexing;
        mod pooling;
        mod sink;
        mod sink_pool;
        mod socket;
        mod stream;

        #[cfg(test)]
        pub mod test_request;

        pub use fluvio_future::net::{BoxConnection,Connection};
        pub use self::error::FlvSocketError;
        pub use self::socket::FlvSocket;
        pub use multiplexing::*;
        pub use pooling::*;
        pub use sink::*;
        pub use sink_pool::*;
        pub use socket::*;
        pub use stream::*;

        use fluvio_protocol::api::Request;
        use fluvio_protocol::api::RequestMessage;
        use fluvio_protocol::api::ResponseMessage;

        /// send request and return response from calling server at socket addr
        pub async fn send_and_receive<R>(
            addr: &str,
            request: &RequestMessage<R>,
        ) -> Result<ResponseMessage<R::Response>, FlvSocketError>
        where
            R: Request,
        {
            let mut client = FlvSocket::connect(addr).await?;

            let msgs: ResponseMessage<R::Response> = client.send(&request).await?;

            Ok(msgs)
        }

    }
}
