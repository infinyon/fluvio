mod k8_dispatcher;
mod k8_ws_service;

pub use k8_dispatcher::*;
pub use k8_ws_service::*;



/*
mod delta{

    use std::collections::HashMap;

    use async_rwlock::RwLock;

    use crate::core::Spec;

    /// WS ActionQueue 
    pub struct WSActionQueue<S> where S: Spec {

        entries: RwLock<HashMap<S::IndexKey,S>>
    }


    impl WSActionQueue  {

        pub fn new() -> Self {
            Self{}
        }

        
    }


    #[cfg(test)]    
    mod test {

        use super::WSActionQueue;

        struct TestSpec {
            pub name: String
        }

        struct TestStatus {

        }




        fn test_insert() {

            let fifo = WSActionQueue::new();


        }

    }

}
*/