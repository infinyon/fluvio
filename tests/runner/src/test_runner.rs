use std::time::Duration;
use fluvio_future::timer::sleep;

use fluvio_system_util::bin::get_fluvio;

use crate::cli::TestOption;
use crate::util::CommandUtil;

/// run test
pub struct TestRunner {
    option: TestOption,
}

impl TestRunner {
    pub fn new(option: TestOption) -> Self {
        Self { option }
    }

    async fn setup_topic(&self) {
        use fluvio::{Fluvio};

        // create topics per replication
        let replication = self.option.replication();

        for i in 0..replication {
            let topic_name = self.option.topic_name(i);
            println!("creating test topic: <{}>", topic_name);
            get_fluvio()
                .expect("fluvio not founded")
                .arg("topic")
                .arg("create")
                .arg(&topic_name)
                .arg("--replication")
                .arg(self.option.replication().to_string())
                .rust_log(self.option.client_log.as_deref())
                .wait_and_check();

            println!("topic: {}, created", topic_name);
        }

        // wait until all partitions are provisioned
        let client = Fluvio::connect().await.expect("should connect");
        let mut admin = client.admin().await;

        for _ in 0..60u16 {
            use fluvio_controlplane_metadata::partition::PartitionSpec;

            let partitions = admin
                .list::<PartitionSpec, _>(vec![])
                .await
                .expect("get partitions status");

            let live_partitions = partitions
                .iter()
                .filter(|par| par.status.is_online())
                .count();

            if live_partitions != replication as usize {
                println!(
                    "partition {} of out of {} is ready, sleeping",
                    live_partitions, replication
                );
                sleep(Duration::from_secs(1)).await;
            } else {
                println!("all {} partitions are live", live_partitions);
                break;
            }
        }

        // print topic and partition status

        get_fluvio()
            .expect("fluvio not founded")
            .arg("topic")
            .arg("list")
            .wait_and_check();

        get_fluvio()
            .expect("fluvio not founded")
            .arg("partition")
            .arg("list")
            .wait_and_check();
    }

    /// main entry point
    #[allow(unused)]
    pub async fn run_test(&self) {
        use std::env;
        use crate::tests::create_test_driver;

        // at this point, cluster is up, we need to ensure clean shutdown of cluster
        // no matter if produce or consumer test crashes

        // topic creation must be done after spu
        // we need to test what happens topic gets created before spu
        if self.option.init_topic() {
            self.setup_topic().await;

            sleep(Duration::from_secs(1)).await;
        } else {
            println!("no topic initialized");
        }

        if self.option.produce.produce_iteration > 0 {
            let test_driver = create_test_driver(self.option.clone());
            test_driver.run().await;
            sleep(Duration::from_millis(200)).await; // let it sleep
        } else {
            println!("no produce iteration, ending");
        }
    }
}
