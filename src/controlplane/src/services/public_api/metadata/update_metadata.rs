use std::sync::Arc;

use std::fmt::Debug;


use event_listener::Event;
use futures::io::AsyncRead;
use futures::io::AsyncWrite;

use kf_protocol::api::*;
use kf_protocol::Encoder;
use fluvio_controlplane_api::metadata::*;
use kf_socket::*;
use flv_future_aio::zero_copy::ZeroCopyWrite;


use crate::core::*;


/// metadata request are handle thru MetadataController which waits metadata event from ConnManager
/// and forward to Client

///
pub struct ClientMetadataController<S> {
    response_sink: InnerExclusiveKfSink<S>,
    context: SharedContext,
    metadata_request: WatchMetadataRequest,
    header: RequestHeader,
    end_event: Arc<Event>,
}

impl<S> ClientMetadataController<S>
where
    S: AsyncWrite + AsyncRead + Unpin + Send + ZeroCopyWrite + 'static,
{
    pub fn handle_metadata_update(
        request: RequestMessage<WatchMetadataRequest>,
        response_sink: InnerExclusiveKfSink<S>,
        end_event: Arc<Event>,
        context: SharedContext,
    ) {
        let (header, metadata_request) = request.get_header_request();
        let controller = Self {
            response_sink,
            context,
            header,
            metadata_request,
            end_event,
        };

        controller.run();
    }

    /// send response using correlation id and version from request
    async fn send_response<P>(&mut self, response: P) -> Result<(), KfSocketError>
    where
        ResponseMessage<P>: Encoder + Default + Debug,
    {
        self.response_sink
            .send_response(
                &ResponseMessage::new(self.header.correlation_id(), response),
                self.header.api_version(),
            )
            .await
    }

    /// send out all metadata to client
    async fn update_all(&mut self) -> Result<(), KfSocketError> {
        /*
        let spus = self.context.spus().clone_values().await;
        let partitions = self.context.partitions().clone_values().await;

        let response = UpdateAllMetadataResponse::new(
            spus.into_iter()
                .map(|spu| spu.into())
                .collect(),
                partitions.into_iter()
            .map(|p| p.into())
            .collect());

        self.send_response(WatchMetadataResponse::All(response)).await
        */
        panic!("not implemented")
    }

    pub fn run(self) {
        use flv_future_aio::task::spawn;

        spawn(self.dispatch_loop());
    }

    async fn dispatch_loop(mut self) {
        use tokio::select;

        /*
        let mut counter: i32 = 0;
        let mut receiver = self.context.store_channnew_client_subscriber();
        let sink_id = self.response_sink.id();
        let correlation_id = self.header.correlation_id();
        let resync_period = self.metadata_request.re_sync_period_ms;

        debug!(
            "starting client meta loop, sink: {}, correlation: {}, resync: {}",
            sink_id, correlation_id, resync_period
        );

        // first send everything
        if let Err(err) = self.update_all().await {
            error!("error sending out initial schema: {}, error: {}, finishing it", sink_id, err);
            return;
        }

        loop {
            counter += 1;
            debug!(
                "waiting on conn: {}: correlation: {}, counter: {}",
                sink_id, correlation_id, counter
            );

            select! {

               _ = self.end_event.listen() => {
                    debug!("socket: {} closed",sink_id);
                    break;
               },
               _ = (sleep(Duration::from_millis(resync_period as u64))) => {

                   debug!("metadata reconciliation: {}, correlation: {}",sink_id,correlation_id);
                   if let Err(err) = self.update_all().await {
                       error!("error updating all schema: {}, error: {}",sink_id,err);
                       break;
                   }

               },
               client_event = receiver.recv() => {

                   match client_event {
                       Ok(value) => {

    
                           match value {
                               ClientNotification::SPU(response) => {

                                   if let Err(err) = self.send_response(WatchMetadataResponse::SPU(response)).await {
                                       error!("error {} sending out spu update: {}",err,sink_id);
                                       break;
                                   }

                               },
                               ClientNotification::Replica(response) => {
                                   if let Err(err) = self.send_response(WatchMetadataResponse::Replica(response)).await {
                                       error!("error {} sending out spu update: {}",err,sink_id);
                                       break;
                                   }
                               },
                               ClientNotification::Topic => {
                                   
                               }

                           };

                       },
                       Err(err) => {
                           match err {
                               RecvError::Closed => {
                                   error!("receiver to conn manager closed!");
                                   break;
                               },
                               RecvError::Lagged(lag) => {
                                   error!("conn: {}, lagging: {}",sink_id,lag);
                                   
                               }

                           }
                       }
                   }

               },

               _ = self.end_event.listen() => {
                   debug!("socket: {}, terminated, ending loop",sink_id);
                   break;
               }


            }
        }
        */

       // debug!("terminating update metadata loop, correlation: {}",correlation_id);
    }
}
