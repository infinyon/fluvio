use std::{env::temp_dir, time::Duration};

use fluvio_controlplane::replica::Replica;
use fluvio_protocol::link::ErrorCode;
use fluvio_spu_schema::server::consumer_offset::GetConsumerOffsetRequest;
use fluvio_types::defaults::CONSUMER_REPLICA_KEY;
use tracing::debug;

use flv_util::fixture::ensure_clean_dir;

use fluvio_future::timer::sleep;
use fluvio_socket::{FluvioSocket, MultiplexerSocket};
use crate::kv::consumer::{ConsumerOffset, ConsumerOffsetKey};
use crate::services::public::tests::create_public_server_with_root_auth;
use crate::core::GlobalContext;
use crate::config::SpuConfig;
use crate::replication::leader::LeaderReplicaState;

use fluvio_protocol::api::RequestMessage;

#[fluvio_future::test(ignore)]
async fn test_get_consumer_offset() {
    let test_path = temp_dir().join("test_stream_fetch");
    ensure_clean_dir(&test_path);
    let port = portpicker::pick_unused_port().expect("No free ports left");

    let addr = format!("127.0.0.1:{port}");
    let mut spu_config = SpuConfig::default();
    spu_config.log.base_dir = test_path;
    let ctx = GlobalContext::new_shared_context(spu_config);

    let server_end_event = create_public_server_with_root_auth(addr.to_owned(), ctx.clone()).run();

    // wait for stream controller async to start
    sleep(Duration::from_millis(100)).await;

    let client_socket =
        MultiplexerSocket::new(FluvioSocket::connect(&addr).await.expect("connect"));

    let topic = "test";
    let target_replica = (topic.to_owned(), 0);

    let consumer_id = "test_consumer";

    let consumer_replica = Replica::new(CONSUMER_REPLICA_KEY.to_owned(), 5001, vec![5001]);
    let consumer_replica =
        LeaderReplicaState::create(consumer_replica, ctx.config(), ctx.status_update_owned())
            .await
            .expect("replica")
            .init(&ctx)
            .await
            .expect("init succeeded");

    ctx.leaders_state()
        .insert(CONSUMER_REPLICA_KEY.into(), consumer_replica.clone())
        .await;

    let consumer_replica = ctx
        .leaders_state()
        .get(&CONSUMER_REPLICA_KEY.into())
        .await
        .expect("replica");

    let consumer_offset = ctx
        .consumer_offset()
        .get_or_insert(&consumer_replica, ctx.follower_notifier())
        .await
        .expect("consumer offset replica");

    let key = ConsumerOffsetKey::new(target_replica.clone(), consumer_id);
    let consumer = ConsumerOffset::new(5);
    consumer_offset.put(key, consumer).await.expect("put");

    let get_consumer_offset_request =
        GetConsumerOffsetRequest::new(target_replica.into(), consumer_id.to_owned());

    let response = client_socket
        .send_and_receive(RequestMessage::new_request(get_consumer_offset_request))
        .await
        .expect("send");

    assert_eq!(response.consumer.expect("consumer").offset, 5);
    assert_eq!(response.error_code, ErrorCode::None);

    server_end_event.notify();
    debug!("terminated controller");
}
