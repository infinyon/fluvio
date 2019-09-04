//!
//! # SC Controller
//!
//! Streaming Coordinator Controller receives messages from other components and
//! dispatches them to internal components: SPUs, Topics, Partitions.

use log::trace;
use log::error;
use log::info;
use futures::select;
use futures::stream::StreamExt;

use types::log_on_err;
use metadata::topic::TopicSpec;
use metadata::spu::SpuSpec;
use future_helper::spawn;

use crate::core::WSUpdateService;
use crate::core::WSChangeChannel;
use crate::core::ShareLocalStores;


use super::TopicReducer;
use super::TopicChangeRequest;


#[derive(Debug)]
pub struct TopicController<W> {
    local_stores: ShareLocalStores,
    ws_service: W,
    topic_receiver: WSChangeChannel<TopicSpec>,
    spu_receiver: WSChangeChannel<SpuSpec>,
    reducer: TopicReducer,
}

impl <W>TopicController<W> 

 where
        W: WSUpdateService + Send + 'static
       
{

     /// streaming coordinator controller constructor
    pub fn new(
        local_stores: ShareLocalStores,
        spu_receiver: WSChangeChannel<SpuSpec>,
        topic_receiver: WSChangeChannel<TopicSpec>,
        ws_service: W) -> Self {


        Self {
            reducer: TopicReducer::new(local_stores.topics().clone(),
                local_stores.spus().clone(),
                local_stores.partitions().clone()
            ),
            local_stores,
            spu_receiver,
            topic_receiver,
            ws_service,
            
        }
    }



    pub fn run(self)  {

        spawn(self.dispatch_loop());
    }



    async fn dispatch_loop(mut self)  {

        loop {
            select! {
                topic_req = self.topic_receiver.next() => {
                    match topic_req {
                        None => {
                            error!("Topic LC dispatcher has been terminated.  Ending Server loop");
                            break;
                        },
                        Some(request) => {
                            trace!("receive Topic request  {:#?}",request);
                            self.process_request(TopicChangeRequest::Topic(request)).await;
                        },
                    }
                },
                spu_req = self.spu_receiver.next() => {
                    match spu_req {
                        None => {
                            error!("SPU lC dispatcher has been terminated. Ending server loop");
                            break;
                        },
                        Some(request) => {
                            trace!("received SPU request {:#?}",request);
                            self.process_request(TopicChangeRequest::Spu(request)).await;
                        }
                    }
                }
                complete => {},
            }
        }

        info!("spu controller is terminated");

    }


  

    /// process requests related to SPU management
    async fn process_request(&mut self, request: TopicChangeRequest) {
        
        match self.reducer.process_requests(request) {
            Ok(actions) => {
                 
                trace!(
                    "Topic actions: {}",
                    actions
                );
    
                if actions.topics.count() > 0 {
                    for ws_action in actions.topics.into_iter() {
                        log_on_err!(
                            self.ws_service.update_topic(ws_action).await
                        );
                    }
                }

                if actions.partitions.count() > 0 {
                    for ws_action in actions.partitions.into_iter() {
                        log_on_err!(
                            self.ws_service.update_partition(ws_action).await
                        )
                    }
                }
            },
            Err(err) => error!("error processing topic spu: {}",err)
        }

      
    }

    
}




/*
#[cfg(test)]
mod tests {

    use log::debug;
    use futures::channel::mpsc::channel;
    use futures::channel::mpsc::Receiver;

    use future_helper::test_async;
    use utils::actions::Actions;
    use metadata::spu::SpuSpec;

    use crate::cli::ScConfig;
    use crate::core::ScMetadata;
    use crate::core::ScContext;
    use crate::core::ScRequest;
    use crate::core::auth_tokens::AuthTokenAction;
    use crate::core::spus::SpuAction;
    use crate::core::spus::SpuKV;
    use crate::tests::fixture::MockConnectionManager;
    use crate::tests::fixture::SharedKVStore;
    use crate::core::send_channels::ScSendChannels;
    use crate::core::common::new_channel;
    use crate::hc_manager::HcAction;

    use super::ScController;

    fn sample_controller() -> (
        ScController<SharedKVStore, MockConnectionManager>,
        (Receiver<ScRequest>, Receiver<Actions<HcAction>>),
    ) {
        let default_config = ScConfig::default();
        let metadata = ScMetadata::shared_metadata(default_config);
        let conn_manager = MockConnectionManager::shared_conn_manager();

        let (sc_sender, sc_receiver) = channel::<ScRequest>(100);
        let (hc_sender, hc_receiver) = new_channel::<Actions<HcAction>>();
        let kv_store = SharedKVStore::new(sc_sender.clone());
        let sc_send_channels = ScSendChannels::new(hc_sender.clone());

        (
            ScController::new(
                ScContext::new(sc_send_channels, conn_manager.clone()),
                metadata.clone(),
                kv_store.clone(),
            ),
            (sc_receiver, hc_receiver),
        )
    }

    /// test add new spu
    #[test_async]
    async fn test_controller_basic() -> Result<(), ()> {
        let (mut controller, _other) = sample_controller();

        let auth_token_actions: Actions<AuthTokenAction> = Actions::default();

        let spu: SpuKV = SpuSpec::new(5000).into();
        let mut spu_actions: Actions<SpuAction> = Actions::default();

        spu_actions.push(SpuAction::AddSpu(spu.spec().name(), spu.clone()));
        /*
        let topic_actions: Actions<TopicAction> = Actions::default();
        for (name,topic) in self.kv.topics.read().iter() {
            topic_actions.push(TopicAction::AddTopic(name.clone(),topic.clone()));
        }

        let partition_actions: Actions<PartitionAction> = Actions::default();
        for (key,partition) in self.kv.partitions.read().iter() {
            partition_actions.push(PartitionAction::AddPartition(key.clone(),partition.clone()));
        }
        */
        controller
            .process_sc_request(ScRequest::UpdateAll(
                auth_token_actions,
                spu_actions,
                Actions::default(),
                Actions::default(),
            ))
            .await;

        let metadata = controller.metadata();
        debug!("metadata: {:#?}", metadata);

        // metdata should container new spu
        assert!(metadata.spus().spu("spu-5000").is_some());

        Ok(())
    }
    

}
*/