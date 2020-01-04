
use log::debug;
use log::trace;


use futures::stream::StreamExt;
use futures::select;

use kf_socket::KfStream;
use kf_socket::KfSink;
use kf_socket::KfSocketError;
use kf_protocol::api::RequestMessage;
use kf_protocol::api::RequestHeader;
use kf_protocol::api::Offset;
use kf_protocol::api::Isolation;
use flv_metadata::partition::ReplicaKey;
use kf_protocol::fs::FilePartitionResponse;
use spu_api::fetch::FileFlvContinuousFetchRequest;
use spu_api::fetch::FlvContinuousFetchResponse;
use spu_api::SpuApiKey;
use spu_api::PublicRequest;

use crate::core::DefaultSharedGlobalContext;


/// continuous fetch handler
/// while client is active, it continuously send back new records
pub struct CfHandler {
    ctx: DefaultSharedGlobalContext,
    replica: ReplicaKey,
    isolation: Isolation,
    header: RequestHeader,
    kf_sink: KfSink
}

impl CfHandler {


    /// handle fluvio continuous fetch request
    /// 
    pub async fn handle_continuous_fetch_request(
        request: RequestMessage<FileFlvContinuousFetchRequest>,
        ctx: DefaultSharedGlobalContext,
        kf_sink: KfSink,
        kf_stream: KfStream
    ) -> Result<(), KfSocketError> {


        // first get receiver to offset update channel to we don't missed events
        

        let (header, msg) = request.get_header_request();

        let current_offset = msg.fetch_offset;
        let isolation = msg.isolation;
        let replica = ReplicaKey::new(msg.topic,msg.partition);

        debug!("start handling cf fetch replica: {} offset: {}",replica,current_offset);

        let mut handler = Self {
            ctx,
            isolation,
            replica,
            header,
            kf_sink
        };

        handler.process(current_offset,kf_stream).await
    }
     
    async fn process(&mut self,starting_offset: Offset,mut kf_stream: KfStream) -> Result<(), KfSocketError> {

        let mut receiver = self.ctx.offset_channel().receiver().fuse();

        let mut current_offset = if let Some(offset) = self.send_back_records(starting_offset).await? {
            offset
        } else {
            debug!("no records, finishing processing");
            return Ok(())
        };
        
        let mut api_stream = kf_stream.api_stream::<PublicRequest,SpuApiKey>().fuse();

        
        loop {

            select! {
                offset_event = receiver.next() => {

                    if let Some(offset_event) = offset_event {
                        if offset_event.replica_id == self.replica {

                            // depends on isolation, we need to keep track different offset
                            let update_offset = match self.isolation {
                                Isolation::ReadCommitted => offset_event.hw,
                                Isolation::ReadUncommitted => offset_event.leo
                            };
                            if update_offset != current_offset {
                                debug!("cf handler updated offset replica: {} offset: {} diff from prev: {}",self.replica,update_offset,current_offset);
                                if let Some(offset) = self.send_back_records(current_offset).await? {
                                    debug!("cf handler: replica: {} read offset: {}",self.replica,offset);
                                    current_offset = offset;
                                } else {
                                    debug!("cf handler: no more replica: {} records can be read",self.replica);
                                    break;
                                }
                            } else {
                                trace!("no changed in offset: {} offset: {} ignoring",self.replica,update_offset);
                            }
                           
                        }
                    }
            
                },
                
                msg = api_stream.next() => {
                    if let Some(content) = msg {
                        debug!("received msg: {:#?}, continue processing",content);
                    } else {
                        debug!("client has disconnected, ending continuous fetching: {}",self.replica);
                        break;
                    }
                   
                }
            }
        }
        
    
        Ok(())
    }


    async fn send_back_records(&mut self,offset: Offset) -> Result<Option<Offset>, KfSocketError> {

        let mut partition_response = FilePartitionResponse::default();
        partition_response.partition_index = self.replica.partition;

        if let Some((hw,leo)) = self.ctx.leaders_state().read_records(
            &self.replica,
            offset,
            self.isolation.clone(),
            &mut partition_response,
        ).await {

            debug!("cf handler retrieved records replica: {}, from: {} to hw: {}, leo: {}",self.replica, offset,hw,leo);
            let response = FlvContinuousFetchResponse {
                topic: self.replica.topic.clone(),
                partition: partition_response
            };
    
            let response =
                RequestMessage::<FileFlvContinuousFetchRequest>::response_with_header(&self.header, response);
            trace!("sending back file fetch response: {:#?}", response);
            
            self.kf_sink.encode_file_slices(&response, self.header.api_version())
                .await?;
    
            trace!("finish sending fetch response");
    
            // get next offset
            let next_offset = match self.isolation {
                Isolation::ReadCommitted => hw,
                Isolation::ReadUncommitted => leo
            };

            Ok(Some(next_offset))
    
        } else {
            debug!("cf handler unable to retrieve records from replica: {}, from: {}",self.replica, offset);
            // in this case, partition is not founded
            Ok(None)
        }
    }


}

