use std::io::Error as IoError;

use log::debug;
use log::info;
use log::error;
use futures::stream::StreamExt;
use futures::io::AsyncReadExt;
use async_std::io::copy;
use flv_future_aio::net::TcpListener;
use flv_future_aio::net::TcpStream;
use flv_future_aio::net::tls::TlsAcceptor;
use flv_future_aio::net::tls::DefaultServerTlsStream;
use flv_future_aio::task::spawn;


/// start TLS proxy at addr to target
pub async fn start(addr: &str, acceptor: TlsAcceptor, target: String) -> Result<(),IoError> {

    let listener = TcpListener::bind(addr).await?;
    info!("proxy started at: {}",addr);
    let mut incoming = listener.incoming();
    while let Some(stream) = incoming.next().await {
        
        debug!("server: got connection from client");
        if let Ok(tcp_stream) = stream {

            debug!(
                "new connection from {}",
                tcp_stream
                    .peer_addr()
                    .map(|addr| addr.to_string())
                    .unwrap_or("".to_owned())
            );
            
            let handshake = acceptor.accept(tcp_stream);

            match handshake.await {
                Ok(inner_stream) => {
                    debug!("handshake success: starting");
                    process_stream(inner_stream,target.clone()).await;
                },
                Err(err) => error!("error handshaking: {}",err)
            }
        }
    }

    info!("server terminated");
    Ok(())
}


async fn process_stream(stream: DefaultServerTlsStream, target: String) {

    // connect to other end
    if let Err(err) = proxy(stream,target).await {
        error!("error processing tls: {}",err);
    }
    
}

async fn proxy(tls_stream: DefaultServerTlsStream, target: String) -> Result<(),IoError> {

    debug!("trying to connect to target at: {}", target);
    let tcp_stream = TcpStream::connect(&target).await?;

    debug!("connect to target: {}",target);
    let (mut target_stream,mut target_sink) = tcp_stream.split();
    let (mut from_tls_stream,mut from_tls_sink) = tls_stream.split();
    
    let source_to_target_ft = async move {

        match copy(&mut from_tls_stream,&mut target_sink).await {
            Ok(_) => {
                debug!("done copying from source to target");
            },
            Err(err) => {
                error!("error copying: {}",err);
            }
        }
    };

    let target_to_source = async move {
        match copy(&mut target_stream,&mut from_tls_sink).await {
            Ok(_) => {
                debug!("done copying from source to target");
            },
            Err(err) => {
                error!("error copying: {}",err);
            }
        }
    };

    spawn(source_to_target_ft);
    spawn(target_to_source);

    Ok(())
}

    
