#[cfg(unix)]
mod server;

#[cfg(test)]
pub mod test_request;

pub use self::server::*;
pub use fluvio_protocol::codec::FluvioCodec;

#[macro_export]
macro_rules! call_service {
    ($req:expr,$handler:expr,$sink:expr,$msg:expr) => {{
        {
            let version = $req.header.api_version();
            tracing::debug!(api = $msg, "invoking handler");
            let response = $handler.await?;
            tracing::trace!("send back response: {:#?}", &response);
            // we do not fast return here because there could be incoming requests to read
            // even if the socket is closed to write.
            if let Err(err) = $sink.send_response(&response, version).await {
                tracing::warn!(
                    "sending response failed: {}. Client could gave up waiting for the response",
                    err
                );
            }
            tracing::debug!(api = $msg, "finished");
        }
    }};

    ($handler:expr,$sink:expr) => {{
        call_service!($handler, $sink, "")
    }};
}

#[macro_export]
macro_rules! api_loop {
    ( $api_stream:ident, $($matcher:pat => $result:expr),*) => {{

        use futures_util::stream::StreamExt;
        loop {

            tracing::debug!("waiting for next api request");
            if let Some(msg) = $api_stream.next().await {
                if let Ok(req_message) = msg {
                    tracing::trace!("received request: {:#?}",req_message);
                    match req_message {
                        $($matcher => $result),*
                    }
                } else {
                    tracing::debug!("no content, end of connection {:#?}", msg);
                    break;
                }

            } else {
                tracing::debug!("client connect terminated");
                break;
            }
        }
    }};

    ( $api_stream:ident, $debug_msg:expr, $($matcher:pat => $result:expr),*) => {{

        use futures_util::stream::StreamExt;
        loop {

            tracing::debug!("waiting for next api request: {}",$debug_msg);
            if let Some(msg) = $api_stream.next().await {
                if let Ok(req_message) = msg {
                    tracing::trace!("received request: {:#?}",req_message);
                    match req_message {
                        $($matcher => $result),*
                    }
                } else {
                    tracing::debug!("no content, end of connection {}", $debug_msg);
                    break;
                }

            } else {
                tracing::debug!("client connect terminated: {}",$debug_msg);
                break;
            }
        }
    }};
}

/// wait for a single request
#[macro_export]
macro_rules! wait_for_request {
    ( $api_stream:ident, $($matcher:pat => $result:expr),+) => {{
        use futures_util::stream::StreamExt;

        if let Some(msg) = $api_stream.next().await {
            if let Ok(req_message) = msg {
                tracing::trace!("received request: {:#?}", req_message);
                match req_message {
                    $($matcher => $result,)+
                    _ => {
                        tracing::error!("unexpected request: {:#?}", req_message);
                        return Ok(());
                    }
                }
            } else {
                tracing::trace!("no content, end of connection");
                return Ok(());
            }
        } else {
            tracing::trace!("client connect terminated");
            return Ok(());
        }
    }};
}
