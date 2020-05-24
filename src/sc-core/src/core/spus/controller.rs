//!
//! # Spu Controller

use log::debug;
use log::info;
use log::trace;
use log::error;
use futures::channel::mpsc::Receiver;
use futures::channel::mpsc::Sender;
use futures::channel::mpsc::channel;
use futures::select;
use futures::stream::StreamExt;

use flv_future_aio::task::spawn;
use flv_util::actions::Actions;
use flv_metadata::spu::SpuSpec;
use flv_types::log_on_err;

use crate::core::WSUpdateService;
use crate::conn_manager::SharedConnManager;
use crate::conn_manager::SpuConnectionStatusChange;
use crate::core::ShareLocalStores;
use crate::core::common::LSChange;
use crate::core::WSChangeChannel;

use super::SpuReducer;
use super::SpuChangeRequest;

#[derive(Debug)]
pub struct SpuController<W> {
    local_stores: ShareLocalStores,
    conn_manager: SharedConnManager,
    ws_service: W,
    spu_reducer: SpuReducer,
    lc_receiver: Receiver<Actions<LSChange<SpuSpec>>>,
    conn_receiver: Receiver<SpuConnectionStatusChange>,
    conn_sender: Sender<SpuConnectionStatusChange>,
}

impl<W> SpuController<W>
where
    W: WSUpdateService + Send + Sync + 'static,
{
    pub fn new(
        local_stores: ShareLocalStores,
        conn_manager: SharedConnManager,
        lc_receiver: WSChangeChannel<SpuSpec>,
        ws_service: W,
    ) -> Self {
        let (conn_sender, conn_receiver) = channel(100);

        Self {
            spu_reducer: SpuReducer::new(local_stores.spus().clone()),
            local_stores: local_stores,
            ws_service: ws_service,
            conn_manager,
            lc_receiver,
            conn_receiver,
            conn_sender,
        }
    }

    pub fn conn_sender(&self) -> Sender<SpuConnectionStatusChange> {
        self.conn_sender.clone()
    }

    pub fn run(self) {
        let ft = async move {
            self.dispatch_loop().await;
        };

        spawn(ft);
    }

    async fn dispatch_loop(mut self) {
        loop {
            select! {
                receiver_req = self.lc_receiver.next() => {
                    match receiver_req {
                        None => {
                            error!("Listener LC dispatcher has been terminated. Ending Server loop");
                            break;
                        },
                        Some(request) => {
                            trace!("received SPU request from ws dispatcher: {:#?}",request);
                            self.process_request(SpuChangeRequest::SpuLS(request)).await;
                        },
                    }
                },
                conn_req = self.conn_receiver.next() => {
                    match conn_req  {
                        None => {
                            error!("Listener to Conn Mgr has been terminated. Ending Server loop");
                            break;
                        },
                        Some(request) => {
                            trace!("received request from conn manager: {:#?}",request);
                            self.process_request(SpuChangeRequest::Conn(request)).await;
                        }
                    }
                }
                complete => {},
            }
        }

        info!("spu controller is terminated");
    }

    /// process requests related to SPU management
    async fn process_request(&mut self, request: SpuChangeRequest) {
        // process SPU action; update context with new actions
        match self.spu_reducer.process_requests(request) {
            Ok(actions) => {
                debug!("SPU Controller apply actions: {}", actions);
                // send actions to kv
                if actions.spus.count() > 0 {
                    for ws_action in actions.spus.into_iter() {
                        log_on_err!(self.ws_service.update_spu(ws_action).await);
                    }
                }
                if actions.conns.count() > 0 {
                    self.conn_manager.process_requests(actions.conns).await;
                }
            }
            Err(err) => error!("error generating spu actions from reducer: {}", err),
        }
    }
}

/*
#[cfg(test)]
mod tests {

    use log::debug;
    use futures::channel::mpsc::channel;
    use futures::channel::mpsc::Receiver;

    use flv_future_core::test_async;
    use utils::actions::Actions;
    use flv_metadata::spu::SpuSpec;

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
