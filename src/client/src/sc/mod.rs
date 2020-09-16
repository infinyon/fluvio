mod controller {
    use tracing::debug;
    use futures::select;
    use futures::channel::mpsc::Sender;

    use fluvio_sc_schema::client::*;
    use kf_socket::*;
    use flv_future_aio::task::spawn;

    use crate::client::ScClient;

    const SC_RECONCILIATION_INTERVAL_SEC: u64 = 60;

    pub struct ScManager {
        shared_sink: ExclusiveAllKfSink,
    }

    impl ScManager {
        pub fn new(sc_client: ScClient) -> Self {
            let inner = sc_client.unwrap();
            let (socket, _, _) = inner.split();
            let (sink, stream) = socket.split();

            let shared_sink = ExclusiveAllKfSink::new(sink);

            ScRequestDispatcher::run(stream);

            Self { shared_sink }
        }
    }

    /// implement simplistic multiplexing dispatcher
    /// there are 3 channels
    /// first is metadata
    /// second other admin API, this assumes that admin call are ordered
    pub struct ScRequestDispatcher {
        stream: AllKfStream,
    }

    impl ScRequestDispatcher {
        pub fn run(stream: AllKfStream) {
            let dispatcher = Self { stream };

            debug!("spawning sc request dispatcher");
            spawn(dispatcher.dispatcher_loop());
        }

        /// perform request loop to client
        async fn dispatcher_loop(mut self) {
            debug!("entering sc request loop");

            // the code is almost same as in the fluvio_spu::controllers::sc::dispatcher
            loop {
                use std::time::Duration;
                use futures::FutureExt;
                use futures::StreamExt;

                use flv_future_aio::timer::sleep;

                debug!("waiting for request from sc");

                let frame_stream = self.stream.get_mut_tcp_stream();
                select! {

                    _ = (sleep(Duration::from_secs(SC_RECONCILIATION_INTERVAL_SEC))).fuse() => {
                        debug!("timer fired - exiting sc request loop");
                        break;
                    },

                    sc_request = frame_stream.next().fuse() => {

                        if let Some(sc_msg) = sc_request {
                            if let Ok(req_bytes) = sc_msg {

                            } else {
                                debug!("no more sc msg content, end");
                                break;
                            }
                        } else {
                            debug!("sc connection terminated");
                            break;
                        }
                    }

                }
            }
        }
    }
}
