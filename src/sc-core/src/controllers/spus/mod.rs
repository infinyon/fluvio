mod actions;
mod controller;

pub use self::actions::*;
pub use self::controller::*;
pub use channel::*;

mod channel {


    use async_channel::{Sender, Receiver, bounded};
    use super::*;

    #[derive(Debug)]
    pub struct SpuStatusChannel {
        sender: Sender<SpuAction>,
        receiver: Receiver<SpuAction>
    }


    impl SpuStatusChannel {

        pub fn new() -> Self {
            let (sender,receiver) = bounded(10);

            Self {
                sender,
                receiver
            }
        }

        pub fn receiver(&self) -> Receiver<SpuAction> {
            self.receiver.clone()
        }

        pub fn sender(&self) -> Sender<SpuAction> {
            self.sender.clone()
        }
    }

    
}