use clap::Parser;

use anyhow::Result;

use fluvio::{RecordKey, TopicProducer, TopicProducerConfigBuilder, FluvioAdmin};
use fluvio_controlplane_metadata::partition::PartitionSpec;

use fluvio_test_derive::fluvio_test;
use fluvio_test_case_derive::MyTestCase;

#[derive(Debug, Clone, Parser, Default, Eq, PartialEq, MyTestCase)]
#[clap(name = "Fluvio Producer Batch Test")]
pub struct ProduceBatchTestOption {}

#[fluvio_test(topic = "batch", async)]
pub async fn produce_batch(
    mut test_driver: Arc<FluvioTestDriver>,
    mut test_case: TestCase,
) -> TestResult {
    println!("Starting produce_batch test");

    let topic_name = test_case.environment.base_topic_name();
    let config = TopicProducerConfigBuilder::default()
        .linger(std::time::Duration::from_millis(10))
        .build()
        .expect("failed to build config");

    let producer: TopicProducer = test_driver
        .create_producer_with_config(&topic_name, config)
        .await;

    println!("Created producer");

    let leader = {
        let admin: FluvioAdmin = test_driver.client().admin().await;
        let partitions = admin.all::<PartitionSpec>().await.expect("partitions");
        let test_topic = &partitions[0];
        test_topic.spec.leader
    };

    println!("Found leader {leader}");

    let cluster_manager = test_driver
        .get_cluster()
        .expect("cluster")
        .env_driver()
        .create_cluster_manager();

    println!("Got cluster manager");

    let value = "a".repeat(5000);
    let result: Result<_> = (|| async move {
        let mut results = Vec::new();
        for _ in 0..1000 {
            let result = producer.send(RecordKey::NULL, value.clone()).await?;
            results.push(result);
        }
        println!("Send 1000");
        let mut i = 0;
        for result in results.into_iter() {
            let record = result.wait().await.expect("result is not ok");

            // Check that offset is correct for each record sent.
            assert_eq!(record.offset(), i);

            i += 1;
        }

        assert_eq!(i, 1000);

        fluvio_future::timer::sleep(std::time::Duration::from_secs(3)).await;

        cluster_manager.terminate_spu(leader).expect("terminate");
        println!("Terminate SPU");

        println!("Sending one record");

        let _result = producer
            .send(RecordKey::NULL, i.to_string())
            .await
            .expect("Failed send");

        println!("flushing");

        // This should fail because the SPU is terminated.
        producer.flush().await?;

        Ok(())
    })()
    .await;

    // Ensure that one of the calls returned a failure
    assert!(result.is_err());
}
