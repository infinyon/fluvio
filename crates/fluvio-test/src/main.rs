use std::sync::Arc;
use std::process::exit;
use structopt::StructOpt;
use fluvio::Fluvio;
use fluvio_test_util::test_meta::{BaseCli, TestCase, TestCli, TestOption};
use fluvio_test_util::test_meta::test_result::TestResult;
use fluvio_test_util::test_meta::environment::{EnvDetail, EnvironmentSetup};
use fluvio_test_util::setup::TestCluster;
use fluvio_future::task::run_block_on;
use std::panic::{self, AssertUnwindSafe};
use fluvio_test_util::test_runner::test_driver::{TestDriver, TestDriverType};
use fluvio_test_util::test_runner::test_meta::FluvioTestMeta;
use fluvio_test_util::test_meta::test_timer::TestTimer;
use hdrhistogram::Histogram;

// This is important for `inventory` crate
#[allow(unused_imports)]
use fluvio_test::tests as _;
use async_lock::RwLock;

fn main() {
    run_block_on(async {
        let option = BaseCli::from_args();
        //println!("{:?}", option);

        let test_name = option.environment.test_name.clone();

        // Get test from inventory
        let test_meta =
            FluvioTestMeta::from_name(&test_name).expect("StructOpt should have caught this error");

        let mut subcommand = vec![test_name.clone()];

        // We want to get a TestOption compatible struct back
        let test_opt: Box<dyn TestOption> = if let Some(TestCli::Args(args)) = option.test_cmd_args
        {
            // Add the args to the subcommand
            subcommand.extend(args);

            // Parse the subcommand
            (test_meta.validate_fn)(subcommand)
        } else {
            // No args
            (test_meta.validate_fn)(subcommand)
        };

        println!("Start running fluvio test runner");
        fluvio_future::subscriber::init_logger();

        // Can I get a FluvioConfig anywhere
        // Deploy a cluster
        let fluvio_client = cluster_setup(&option.environment).await;

        // Check on test requirements before running the test
        if !TestDriver::is_env_acceptable(
            &(test_meta.requirements)(),
            &TestCase::new(option.environment.clone(), test_opt.clone()),
        ) {
            exit(-1);
        }

        let _panic_timer = TestTimer::start();
        /*
        std::panic::set_hook(Box::new(move |panic_info| {
            let mut panic_timer = panic_timer.clone();
            panic_timer.stop();

            let test_result = TestResult {
                success: false,
                duration: panic_timer.duration(),
                ..Default::default()
            };
            //run_block_on(async { cluster_cleanup(panic_options.clone()).await });
            eprintln!("Test panicked: {:#?}",panic_info);
            eprintln!("{}", test_result);
        }));
        */

        let test_result = run_test(
            option.environment.clone(),
            test_opt,
            test_meta,
            fluvio_client,
        )
        .await;
        cluster_cleanup(option.environment).await;

        println!("{}", test_result)
    });
}

async fn run_test(
    environment: EnvironmentSetup,
    test_opt: Box<dyn TestOption>,
    test_meta: &FluvioTestMeta,
    test_driver: Arc<RwLock<TestDriver>>,
) -> TestResult {
    let test_case = TestCase::new(environment, test_opt);
    let test_result = panic::catch_unwind(AssertUnwindSafe(move || {
        (test_meta.test_fn)(test_driver, test_case)
    }))
    .expect("Panic hook should have caught this");

    test_result.expect("Test Result")
}

async fn cluster_cleanup(option: EnvironmentSetup) {
    if option.skip_cluster_delete() {
        println!("skipping cluster delete\n");
    } else {
        let mut setup = TestCluster::new(option.clone());
        setup.remove_cluster().await;
    }
}

async fn cluster_setup(option: &EnvironmentSetup) -> Arc<RwLock<TestDriver>> {
    let fluvio_client = if option.skip_cluster_start() {
        println!("skipping cluster start");
        // Connect to cluster in profile
        Arc::new(TestDriverType::Fluvio(
            Fluvio::connect()
                .await
                .expect("Unable to connect to Fluvio test cluster via profile"),
        ))
    } else {
        let mut test_cluster = TestCluster::new(option.clone());
        Arc::new(TestDriverType::Fluvio(
            test_cluster
                .start()
                .await
                .expect("Unable to connect to fresh test cluster"),
        ))
    };

    Arc::new(RwLock::new(TestDriver {
        admin_client: fluvio_client,
        topic_num: 0,
        producer_num: 0,
        consumer_num: 0,
        producer_bytes: 0,
        consumer_bytes: 0,
        producer_latency_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
        consumer_latency_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
        topic_create_latency_histogram: Histogram::<u64>::new_with_bounds(1, u64::MAX, 2).unwrap(),
    }))
}

#[cfg(test)]
mod tests {
    // CLI Tests

    use std::time::Duration;
    use structopt::StructOpt;
    use fluvio_test_util::test_meta::{BaseCli, TestCli};
    use fluvio_test_util::test_meta::environment::EnvDetail;
    use fluvio_test::tests::smoke::SmokeTestOption;

    #[test]
    fn valid_test_name() {
        let args = BaseCli::from_iter_safe(vec!["fluvio-test", "smoke"]);

        assert!(args.is_ok());
    }

    #[test]
    fn invalid_test_name() {
        let args = BaseCli::from_iter_safe(vec!["fluvio-test", "testdoesnotexist"]);

        assert!(args.is_err());
    }

    #[test]
    fn extra_vars() {
        let args = BaseCli::from_iter(vec![
            "fluvio-test",
            "smoke",
            "--",
            "--producer-iteration=9000",
            "--producer-record-size=1000",
        ]);

        if let Some(TestCli::Args(cmd)) = args.test_cmd_args {
            // Structopt commands expect an arbitrary binary name before the test args
            let mut subcommand = vec![args.environment.test_name.clone()];
            subcommand.extend(cmd);

            let smoke_test_case = SmokeTestOption::from_iter(subcommand);

            let expected = SmokeTestOption {
                producer_iteration: 9000,
                producer_record_size: 1000,
                ..Default::default()
            };

            assert_eq!(smoke_test_case, expected);
        } else {
            panic!("test args not parsed")
        }
    }

    #[test]
    fn topic() {
        let args = BaseCli::from_iter(vec![
            "fluvio-test",
            "smoke",
            "--topic-name",
            "not_the_default_topic_name",
        ]);

        assert_eq!(args.environment.topic_name(), "not_the_default_topic_name");
    }

    #[test]
    fn spu() {
        let args = BaseCli::from_iter(vec!["fluvio-test", "smoke", "--spu", "5"]);

        assert_eq!(args.environment.spu, 5);
    }

    #[test]
    fn timeout() {
        let args = BaseCli::from_iter(vec!["fluvio-test", "smoke", "--timeout", "9000"]);

        assert_eq!(args.environment.timeout(), Duration::from_secs(9000));
    }

    //// We validate that the behavior of cluster_setup and cluster_cleanup work as expected
    //// The clusters are the same if cluster addr from the first run is the same as the second run
    ////
    //// You may need to run `make minikube_image` if this fails with:
    //// 'Failed to install k8 cluster: InstallK8(HelmChartNotFound("fluvio/fluvio-app:a.b.c-xyz"))'
    //// This won't work with k8 until we fix: https://github.com/infinyon/fluvio/issues/859
    ////
    //// Local cluster does not work in `cargo test`:
    //// 'Failed to install local cluster: InstallLocal(IoError(Os { code: 2, kind: NotFound, message: "No such file or directory" }))'
    //#[test]
    //#[ignore]
    //fn skip_cluster_delete_then_skip_cluster_start() {
    //    use super::*;
    //    use fluvio::config::ConfigFile;
    //    use fluvio_future::task::run_block_on;

    //    run_block_on(async {
    //        let skip_cluster_delete_cmd = CliArgs::from_iter(vec![
    //            "fluvio-test",
    //            "smoke",
    //            "--keep-cluster",
    //            "--local",
    //            "--develop",
    //            "--skip-checks",
    //        ]);
    //        let test_case: TestCase = skip_cluster_delete_cmd.into();
    //        let _fluvio_client = cluster_setup(&test_case).await;
    //        let cluster_addr_1 = {
    //            let config_file = ConfigFile::load_default_or_new().expect("Default config");
    //            let fluvio_config = config_file.config().current_cluster().unwrap();
    //            fluvio_config.endpoint.clone()
    //        };
    //        cluster_cleanup(test_case).await;

    //        let skip_cluster_start_cmd = CliArgs::from_iter(vec![
    //            "fluvio-test",
    //            "smoke",
    //            "--disable-install",
    //            "--local",
    //            "--develop",
    //            "--skip-checks",
    //        ]);
    //        let test_case: TestCase = skip_cluster_start_cmd.into();
    //        let _fluvio_client = cluster_setup(&test_case).await;
    //        let cluster_addr_2 = {
    //            let config_file = ConfigFile::load_default_or_new().expect("Default config");
    //            let fluvio_config = config_file.config().current_cluster().unwrap();
    //            fluvio_config.endpoint.clone()
    //        };
    //        cluster_cleanup(test_case).await;

    //        assert_eq!(cluster_addr_1, cluster_addr_2);
    //    });
    //}
}
