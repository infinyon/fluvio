use futures::Future;
use futures::channel::mpsc::Receiver;

use utils::actions::Actions;
use metadata::spu::SpuSpec; 
use metadata::topic::TopicSpec;
use metadata::partition::PartitionSpec;

use crate::ScServerError;
use crate::core::common::WSAction;
use crate::core::common::LSChange;


/// Update the world state
pub trait WSUpdateService {

    type ResponseFuture: Send + Future<Output = Result<(), ScServerError>> + 'static;

    fn update_spu(&self,ws_actions: WSAction<SpuSpec>) -> Self::ResponseFuture;
    fn update_topic(&self,ws_actions: WSAction<TopicSpec>) -> Self::ResponseFuture;
    fn update_partition(&self,ws_actions: WSAction<PartitionSpec>) -> Self::ResponseFuture;

}


pub type WSChangeChannel<S> = Receiver<Actions<LSChange<S>>>;


pub trait WSChangeDispatcher {

    fn create_spu_channel(&mut self) -> WSChangeChannel<SpuSpec>;

    fn create_topic_channel(&mut self) -> WSChangeChannel<TopicSpec>;

    fn create_partition_channel(&mut self) -> WSChangeChannel<PartitionSpec>;
}
