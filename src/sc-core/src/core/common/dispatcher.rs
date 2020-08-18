use std::fmt::Debug;

use tracing::trace;

use tracing::info;
use futures::channel::mpsc::Receiver;
use futures::stream::StreamExt;
use futures::select;

use flv_future_core::spawn;

use utils::actions::Actions;

use crate::core::Status;
use crate::core::Spec;
use crate::core::WSUpdateService;

pub trait ControllerAction {}

pub trait Controller {
    type Spec: Spec;
    type Status: Status;
    type Action: Debug;
}

#[allow(dead_code)]
struct Dispatcher<C>(C);

#[allow(dead_code)]
impl<S> Dispatcher<C>
where
    C: Controller + Send + Sync + 'static,
    C::Action: Send + Sync + 'static,
{
    /// start the controller with ctx and receiver
    pub fn run<K>(controller: C, receiver: Receiver<Actions<C::Action>>, kv_service: K)
    where
        K: WSUpdateService + Send + Sync + 'static,
    {
        spawn(Self::request_loop(receiver, kv_service, controller).await);
    }

    async fn request_loop<K>(
        mut receiver: Receiver<Actions<C::Action>>,
        _kv_service: K,
        mut _controller: C,
    ) where
        K: WSUpdateService + Send + Sync + 'static,
    {
        loop {
            select! {

                receiver_req = receiver.next() =>  {

                    match receiver_req {

                        None =>  {
                            info!("receiver has terminated");
                            break;
                        },

                        Some(request) => {
                            trace!("received actions: {:#?}",request);
                            //controller.process_auth_token_request(&kv_service,request).await;

                        }

                    }

                }
            }
        }
    }
}
