use std::convert::TryInto;
use std::net::SocketAddr;
use std::time::Duration;

use log::debug;
use log::trace;
use futures::future::join;
use futures::future::join_all;
use futures::channel::mpsc::channel;
use futures::channel::mpsc::Sender;
use futures::SinkExt;

use future_helper::sleep;
use kf_socket::KfSocketError;
use kf_socket::KfSocket;
use kf_protocol::api::Offset;
use kf_protocol::api::Request;
use kf_protocol::api::RequestMessage;
//use kf_protocol::api::ResponseMessage;
use kf_protocol::message::produce::DefaultKfProduceRequest;
use kf_protocol::message::produce::DefaultKfPartitionRequest;
use kf_protocol::message::produce::DefaultKfTopicRequest;
use kf_protocol::message::fetch::DefaultKfFetchRequest;
use kf_protocol::message::fetch::FetchPartition;
use kf_protocol::message::fetch::KfFetchRequest;
use kf_protocol::message::fetch::FetchableTopic;
use kf_protocol::api::DefaultBatch;
use kf_protocol::api::DefaultRecord;
use internal_api::messages::UpdateAllSpusMsg;
use internal_api::messages::UpdateAllSpusContent;
use internal_api::messages::Replica;
use internal_api::UpdateSpuRequest;
use metadata::partition::ReplicaKey;
use types::SpuId;
use metadata::spu::SpuSpec;
use metadata::spu::Endpoint;

use super::TestGenerator;
use super::SpuServer;
use super::FlvSystemTest;

pub struct SpuTestRunner<T> where T: FlvSystemTest {
    client_id: String,
    servers: Vec<<<T as FlvSystemTest>::EnvGenerator as TestGenerator>::SpuServer>,
    senders: Vec<Sender<bool>> 
}

impl <T>SpuTestRunner<T> where T: FlvSystemTest,
        <<T as FlvSystemTest>::EnvGenerator as TestGenerator>::SpuServer: From<SpuSpec>
{


    pub async fn run(client_id: String, config: T) -> Result<(),KfSocketError>  {

        let generator = config.env_configuration();

        let mut servers = vec![];
        let mut futures = vec![];
        let mut senders = vec![];
      
    
        for i in 0..config.followers() + 1 {
            let spu = generator.create_spu(i as u16);
            let (sender,receiver) = channel::<bool>(1);
            let server = generator.create_server(&spu)?;
            futures.push(server.run_shutdown(receiver));
            senders.push(sender);
          
            servers.push(spu.into());
        }

        
        let runner = SpuTestRunner {
            client_id,
            servers,
            senders
        };
    

        join(
            runner.run_test(config),
            join_all(futures)
        )
        .await;
    
        Ok(())
    }

    async fn run_test(self,config: T)  {

        //  wait until controller start up
        sleep(Duration::from_millis(10)).await.expect("panic");

        let mut runner = config.main_test(self).await.expect("test should run");
        runner.terminate_server().await;
    }

    // terminating server
    async fn terminate_server(&mut self) {
         // terminate servers
        for i in 0..self.servers.len() {
            let server = &self.servers[i];
            let sender = &mut self.senders[i];

            debug!("terminating server: {}",server.id());
            sender
                .send(true)
                .await
                .expect("shutdown should work");

        }
       
    }


    pub fn leader(&self) -> &<<T as FlvSystemTest>::EnvGenerator as TestGenerator>::SpuServer {
        &self.servers[0]
    }

    pub fn leader_spec(&self) -> &SpuSpec {
        self.leader().spec()
    }

    pub fn followers_count(&self) -> usize {
        self.servers.len() -1
    }

    pub fn follower_spec(&self,index: usize) -> &SpuSpec {
        self.servers[index+1].spec()
    }


    pub fn spu_metadata(&self) -> UpdateAllSpusContent {

        let mut spu_metadata = UpdateAllSpusContent::default();

        for server in &self.servers {
            spu_metadata.mut_add_spu_content(server.spec());
        }

        spu_metadata
    }

    pub fn replica_ids(&self) -> Vec<SpuId> {
        self.servers.iter().map(|follower| follower.spec().id).collect()
    } 

    pub fn replica_metadata(&self,replica: &ReplicaKey) -> Replica {

        let leader_id = self.leader_spec().id;

        Replica {
            replica: replica.clone(),
            leader: leader_id,
            live_replicas: self.replica_ids()
        }
    }

    
    pub async fn send_metadata_to_all<'a>(&'a self,replica: &'a ReplicaKey) -> Result<(),KfSocketError> {

        let spu_metadata = self.spu_metadata().add_replica(self.replica_metadata(replica));
        
        for server in &self.servers {
            let spu_id = server.spec().id;
            let _spu_req_msg = RequestMessage::new_request(UpdateSpuRequest::encode_request(
                    UpdateAllSpusMsg::with_content(spu_id,spu_metadata.clone()),
                ))
                .set_client_id(self.client_id.clone());
            trace!("sending spu metadata to server: {}",spu_id);
           // send_to_endpoint(server.private_endpoint(),&spu_req_msg).await;
           
        }

        debug!("sleeping to allow controllers to catch up with messages");
        sleep(Duration::from_millis(50)).await.expect("panic");
        debug!("woke up, start testing");

        Ok(())
    }
    


    pub fn create_producer_msg<S>(&self,msg: S, topic: S,partition: i32) -> RequestMessage<DefaultKfProduceRequest> 
        where S: Into<String>
    {
        let msg_string: String = msg.into();
        let record: DefaultRecord = msg_string.into();
        let mut batch = DefaultBatch::default();
        batch.records.push(record);

        let mut topic_request = DefaultKfTopicRequest::default();
        topic_request.name = topic.into();
        let mut partition_request = DefaultKfPartitionRequest::default();
        partition_request.partition_index = partition;
        partition_request.records.batches.push(batch);
        topic_request.partitions.push(partition_request);
        let mut req = DefaultKfProduceRequest::default();
        req.topics.push(topic_request);

        RequestMessage::new_request(req).set_client_id(self.client_id.clone())
    }

    pub fn create_fetch_request<S>(&self,offset: Offset, topic: S, partition: i32 ) -> RequestMessage<DefaultKfFetchRequest> 
        where S: Into<String>
    {
        let mut request: DefaultKfFetchRequest = KfFetchRequest::default();
        let mut part_request = FetchPartition::default();
        part_request.partition_index = partition;
        part_request.fetch_offset = offset;

        let mut topic_request = FetchableTopic::default();
        topic_request.name = topic.into();
        topic_request.fetch_partitions.push(part_request);

        request.topics.push(topic_request);

        RequestMessage::new_request(request).set_client_id("test_client")
    }



}

#[allow(dead_code)]
pub async fn send_to_endpoint<'a,R>(endpoint: &'a Endpoint, req_msg: &'a RequestMessage<R>) -> Result<(), KfSocketError> where R: Request,
{
    debug!(
        "client: trying to connect to private endpoint: {:#?}",
        endpoint
    );
    let socket: SocketAddr = endpoint.try_into()?;
    let mut socket = KfSocket::connect(&socket).await?;
    debug!("connected to internal endpoint {:#?}", endpoint);
    let res_msg = socket.send(&req_msg).await?;
    debug!("response: {:#?}", res_msg);
    Ok(())
}
