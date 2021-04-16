use std::sync::Arc;
use std::process::exit;
use std::time::Duration;
use structopt::StructOpt;
use fluvio::Fluvio;
use fluvio_test_util::test_meta::{BaseCli, TestCase, TestCli, TestOption, TestResult};
use fluvio_test_util::test_meta::environment::{EnvDetail, EnvironmentSetup};
use fluvio_test_util::setup::TestCluster;
use fluvio_future::task::run_block_on;
use std::panic::{self, AssertUnwindSafe};
use fluvio_test_util::test_runner::FluvioTest;
use fluvio_test_util::test_meta::TestTimer;
use bencher::bench;
use prettytable::{table, row, cell};

// This is important for `inventory` crate
#[allow(unused_imports)]
use flv_test::tests as _;

fn main() {
    run_block_on(async {
        let option = BaseCli::from_args();
        //println!("{:?}", option);

        let test_name = option.environment.test_name.clone();

        // Get test from inventory
        let test =
            FluvioTest::from_name(&test_name).expect("StructOpt should have caught this error");

        let mut subcommand = vec![test_name.clone()];

        // We want to get a TestOption compatible struct back
        let test_opt: Box<dyn TestOption> = if let Some(TestCli::Args(args)) = option.test_cmd_args
        {
            // Add the args to the subcommand
            subcommand.extend(args);

            // Parse the subcommand
            (test.validate_fn)(subcommand)
        } else {
            // No args
            (test.validate_fn)(subcommand)
        };

        println!("Start running fluvio test runner");
        fluvio_future::subscriber::init_logger();

        // Deploy a cluster
        let fluvio_client = cluster_setup(&option.environment).await;

        // Select test
        let test = inventory::iter::<FluvioTest>
            .into_iter()
            .find(|t| t.name == test_name.as_str())
            .expect("Test not found");

        // Check on test requirements before running the test
        if !FluvioTest::is_env_acceptable(
            &(test.requirements)(),
            &TestCase::new(option.environment.clone(), test_opt.clone()),
        ) {
            exit(-1);
        }

        let panic_timer = TestTimer::start();
        std::panic::set_hook(Box::new(move |_panic_info| {
            let mut panic_timer = panic_timer.clone();
            panic_timer.stop();

            let test_result = TestResult {
                success: false,
                duration: panic_timer.duration(),
            };
            //run_block_on(async { cluster_cleanup(panic_options.clone()).await });
            eprintln!("Test panicked:\n");
            eprintln!("{}", test_result);
        }));

        if option.environment.benchmark {
            run_benchmark(test, fluvio_client, option.environment.clone(), test_opt)
        } else {
            let test_result =
                run_test(option.environment.clone(), test_opt, test, fluvio_client).await;
            cluster_cleanup(option.environment).await;

            println!("{}", test_result)
        }
    });
}

async fn run_test(
    environment: EnvironmentSetup,
    test_opt: Box<dyn TestOption>,
    test: &FluvioTest,
    fluvio_client: Arc<Fluvio>,
) -> TestResult {
    let test_case = TestCase::new(environment, test_opt);
    let test_result = panic::catch_unwind(AssertUnwindSafe(move || {
        (test.test_fn)(fluvio_client.clone(), test_case)
    }))
    .expect("Panic hook should have caught this");

    test_result.expect("Test Result")
}

fn run_benchmark(
    test_fn: &FluvioTest,
    client: Arc<Fluvio>,
    env: EnvironmentSetup,
    opts: Box<dyn TestOption>,
) {
    println!("Starting benchmark test (Usual test output may be silenced)");

    bench::run_once(move |b| {
        let summary = b.auto_bench(move |inner_b| {
            let test_case = TestCase::new(env.clone(), opts.clone());
            inner_b.iter(|| (test_fn.test_fn)(client.clone(), test_case.clone()))
        });

        let elapsed_duration = format!("{:?}", Duration::from_nanos(b.ns_elapsed()));
        let iter_duration = format!("{:?}", Duration::from_nanos(b.ns_per_iter()));

        let table = table!(
            [b->"Perf Test Summary", "Measurement"],
            ["Total time elapsed", elapsed_duration],
            ["Total time elapsed (ns)", b.ns_elapsed()],
            ["Time per iteration", iter_duration],
            ["Time per iteration (ns)", b.ns_per_iter()],
            ["# of iteration", (b.ns_elapsed() / b.ns_per_iter())],
            ["Sum (ns)", summary.sum],
            ["Min (ns)", summary.min],
            ["Max (ns)", summary.max],
            ["Mean (ns)", summary.mean],
            ["Median (ns)", summary.median],
            ["Variance", summary.var],
            ["Standard Deviation", summary.std_dev],
            ["Standard Deviation (%)", summary.std_dev_pct],
            ["Median Absolute Deviation", summary.median_abs_dev],
            ["Median Absolute Deviation (%)", summary.median_abs_dev_pct],
            ["Quartiles", format!("{:?}",summary.quartiles)],
            ["Interquartile Range", summary.iqr]
        );

        println!("{}", table)
    })
}

async fn cluster_cleanup(option: EnvironmentSetup) {
    if option.skip_cluster_delete() {
        println!("skipping cluster delete\n");
    } else {
        let mut setup = TestCluster::new(option.clone());
        setup.remove_cluster().await;
    }
}

async fn cluster_setup(option: &EnvironmentSetup) -> Arc<Fluvio> {
    let fluvio_client = if option.skip_cluster_start() {
        println!("skipping cluster start");
        // Connect to cluster in profile
        Arc::new(
            Fluvio::connect()
                .await
                .expect("Unable to connect to Fluvio test cluster via profile"),
        )
    } else {
        let mut test_cluster = TestCluster::new(option.clone());
        Arc::new(
            test_cluster
                .start()
                .await
                .expect("Unable to connect to fresh test cluster"),
        )
    };

    fluvio_client
}

#[cfg(test)]
mod tests {
    // CLI Tests

    use structopt::StructOpt;
    use fluvio_test_util::test_meta::{BaseCli, TestCli};
    use fluvio_test_util::test_meta::environment::EnvDetail;
    use flv_test::tests::smoke::SmokeTestOption;
    use std::time::Duration;

    #[test]
    fn valid_test_name() {
        let args = BaseCli::from_iter_safe(vec!["flv-test", "smoke"]);

        assert!(args.is_ok());
    }

    #[test]
    fn invalid_test_name() {
        let args = BaseCli::from_iter_safe(vec!["flv-test", "testdoesnotexist"]);

        assert!(args.is_err());
    }

    #[test]
    fn extra_vars() {
        let args = BaseCli::from_iter(vec![
            "flv-test",
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
            "flv-test",
            "smoke",
            "--topic-name",
            "not_the_default_topic_name",
        ]);

        assert_eq!(args.environment.topic_name, "not_the_default_topic_name");
    }

    #[test]
    fn spu() {
        let args = BaseCli::from_iter(vec!["flv-test", "smoke", "--spu", "5"]);

        assert_eq!(args.environment.spu, 5);
    }

    #[test]
    fn timeout() {
        let args = BaseCli::from_iter(vec!["flv-test", "smoke", "--timeout", "9000"]);

        assert_eq!(args.environment.timeout(), Duration::from_secs(9000));
    }

    #[test]
    fn benchmark() {
        let args = BaseCli::from_iter(vec!["flv-test", "smoke", "--benchmark"]);

        assert!(args.environment.is_benchmark());
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
    //            "flv-test",
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
    //            "flv-test",
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
