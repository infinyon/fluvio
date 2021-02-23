use std::sync::Arc;
#[tokio::main]
async fn main() {
    // catch panic in the spawn
    std::panic::set_hook(Box::new(|panic_info| {
        eprintln!("panic {}", panic_info);
        std::process::exit(-1);
    }));

    use fluvio_test_util::test_meta::TestOption;
    use fluvio::Fluvio;

    println!("Start running fluvio test runner");
    fluvio_future::subscriber::init_logger();

    let option = TestOption::parse_cli_or_exit();

    // Deploy a cluster
    let fluvio_client = if option.skip_cluster_start() {
        println!("skipping cluster start");
        // Connect to cluster in profile
        Arc::new(
            Fluvio::connect()
                .await
                .expect("Unable to connect to Fluvio test cluster via profile"),
        )
    } else {
        let mut test_cluster = fluvio_test_util::setup::TestCluster::new(option.clone());
        Arc::new(
            test_cluster
                .start()
                .await
                .expect("Unable to connect to fresh test cluster"),
        )
    };

    // TODO: Build this with Test Runner
    if let Some(test_name) = &option.test_name {
        match test_name.as_str() {
            "smoke" => flv_test::tests::smoke::run(fluvio_client, option.clone()).await,
            "concurrent" => flv_test::tests::concurrent::run(fluvio_client, option.clone()).await,
            "many_producers" => {
                flv_test::tests::many_producers::run(fluvio_client, option.clone()).await
            }
            _ => panic!("Test not found"),
        };
    } else {
        println!("No test run")
    }

    // Cluster cleanup
    if option.skip_cluster_delete() {
        println!("skipping cluster delete");
    } else {
        let mut setup = fluvio_test_util::setup::TestCluster::new(option.clone());
        setup.remove_cluster().await;
    }
}
