#![feature(generators)]

mod kf_server;

#[cfg(test)]
pub mod test_request;

pub use kf_protocol::transport::KfCodec;
pub use self::kf_server::KfApiServer;
pub use self::kf_server::KfService;

#[macro_export]
macro_rules! call_service {
    ($req:expr,$handler:expr,$sink:expr,$msg:expr) => {{
        {
            let version = $req.header.api_version();
            log::trace!("invoking handler: {}", $msg);
            let response = $handler.await?;
            log::trace!("send back response: {:#?}", &response);
            $sink.send_response(&response, version).await?;
            log::trace!("finish send");
        }
    }};

    ($handler:expr,$sink:expr) => {{
        call_service!($handler, $sink, "")
    }};
}

#[macro_export]
macro_rules! api_loop {
    ( $api_stream:ident, $($matcher:pat => $result:expr),*) => {{

        use futures::stream::StreamExt;
        loop {

            log::debug!("waiting for next api request");
            if let Some(msg) = $api_stream.next().await {
                if let Ok(req_message) = msg {
                    log::trace!("received request: {:#?}",req_message);
                    match req_message {
                        $($matcher => $result),*
                    }
                } else {
                    log::debug!("no content, end of connection {:#?}", msg);
                    break;
                }

            } else {
                log::debug!("client connect terminated");
                break;
            }
        }
    }};
}

/// wait for a single request
#[macro_export]
macro_rules! wait_for_request {
    ( $api_stream:ident, $matcher:pat => $result:expr) => {{
        use futures::stream::StreamExt;

        if let Some(msg) = $api_stream.next().await {
            if let Ok(req_message) = msg {
                log::trace!("received request: {:#?}", req_message);
                match req_message {
                    $matcher => $result,
                    _ => {
                        log::error!("unexpected request: {:#?}", req_message);
                        return Ok(());
                    }
                }
            } else {
                log::trace!("no content, end of connection");
                return Ok(());
            }
        } else {
            log::trace!("client connect terminated");
            return Ok(());
        }
    }};
}
