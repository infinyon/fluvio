

pub use async_std::sync::Sender;
pub use async_std::sync::Receiver;
pub use inner::Channel;
pub use async_std::sync::RwLock;
pub use async_std::sync::RwLockReadGuard;
pub use async_std::sync::RwLockWriteGuard;


mod inner {

    use async_std::sync::channel;
    use super::Sender;
    use super::Receiver;

    /// abstraction for multi sender receiver channel
    #[derive(Debug)]
    pub struct Channel<T> {
        receiver: Receiver<T>,
        sender: Sender<T>
    }

    impl <T>Channel<T> {

        pub fn new(capacity: usize) -> Self {

            let (sender,receiver) = channel(capacity);
            Self {
                receiver,
                sender
            }
        }

        /// create new clone of sender
        pub fn sender(&self) -> Sender<T> {
            self.sender.clone()
        }

        pub fn receiver(&self) -> Receiver<T> {
            self.receiver.clone()
        }
    }

}
