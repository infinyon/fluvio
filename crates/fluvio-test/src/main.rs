use std::process::exit;
use structopt::StructOpt;
use fluvio::Fluvio;
use fluvio_test_util::test_meta::{BaseCli, TestCase, TestCli, TestOption};
use fluvio_test_util::test_meta::test_result::TestResult;
use fluvio_test_util::test_meta::environment::{EnvDetail, EnvironmentSetup};
use fluvio_test_util::setup::TestCluster;
use std::panic::{self, AssertUnwindSafe};
use fluvio_test_util::test_runner::test_driver::{TestDriver};
use fluvio_test_util::test_runner::test_meta::FluvioTestMeta;
use fluvio_test_util::test_meta::test_timer::TestTimer;
use fluvio_test_util::async_process;
//use tracing::debug;

use nix::unistd::Pid;
use nix::sys::signal::{kill, Signal};

// This is important for `inventory` crate
#[allow(unused_imports)]
use fluvio_test::tests as _;

fn main() {
    let option = BaseCli::from_args();
    //println!("{:?}", option);

    let test_name = option.environment.test_name.clone();

    // Get test from inventory
    let test_meta =
        FluvioTestMeta::from_name(&test_name).expect("StructOpt should have caught this error");

    let mut subcommand = vec![test_name.clone()];

    if let Some(TestCli::Args(args)) = option.test_cmd_args {
        // Add the args to the subcommand
        subcommand.extend(args);
    }

    // We want to get a TestOption compatible struct back
    let test_opt: Box<dyn TestOption> = (test_meta.validate_fn)(subcommand);

    println!("Start running fluvio test runner");
    fluvio_future::subscriber::init_logger();

    // Test connecting to a cluster
    // Deploy a cluster if requested
    cluster_setup(&option.environment).expect("Failed to connect to a cluster");

    // Check on test requirements before running the test
    if !TestDriver::is_env_acceptable(
        &(test_meta.requirements)(),
        &TestCase::new(option.environment.clone(), test_opt.clone()),
    ) {
        exit(-1);
    }

    let panic_timer = TestTimer::start();
    std::panic::set_hook(Box::new(move |panic_info| {
        let mut panic_timer = panic_timer.clone();
        panic_timer.stop();

        let test_result = TestResult {
            success: false,
            duration: panic_timer.duration(),
            ..Default::default()
        };
        //run_block_on(async { cluster_cleanup(panic_options.clone()).await });
        eprintln!("Test panicked: {:#?}", panic_info);
        eprintln!("{}", test_result);
    }));

    let test_result = run_test(option.environment.clone(), test_opt, test_meta);
    cluster_cleanup(option.environment);
    println!("{}", test_result);
}

fn run_test(
    environment: EnvironmentSetup,
    test_opt: Box<dyn TestOption>,
    test_meta: &FluvioTestMeta,
) -> TestResult {
    let test_cluster_opts = TestCluster::new(environment.clone());
    let test_driver = TestDriver::new(Some(test_cluster_opts));

    let test_case = TestCase::new(environment, test_opt);

    if std::env::var("CI").is_ok() {
        (test_meta.test_fn)(test_driver, test_case).unwrap()
    } else {
        let test_result = panic::catch_unwind(AssertUnwindSafe(move || {
            (test_meta.test_fn)(test_driver, test_case)
        }));

        // If we've panicked from the test, we need to terminate all the child processes too to stop the test
        match test_result {
            Ok(r) => r.unwrap(),
            Err(_) => {
                // nix uses pid 0 to refer to the group process, so reap the child processes
                let pid = Pid::from_raw(0);
                kill(pid, Signal::SIGTERM).expect("Unable to kill test process");
                exit(1);
            }
        }
    }
}

fn cluster_cleanup(option: EnvironmentSetup) {
    if option.cluster_delete() {
        let mut setup = TestCluster::new(option);

        let cluster_cleanup_wait = async_process!(async {
            setup.remove_cluster().await;
        });
        let _ = cluster_cleanup_wait
            .join()
            .expect("Cluster cleanup wait failed");
    }
}

// FIXME: Need to confirm SPU options count match cluster. Offer self-correcting behavior
fn cluster_setup(option: &EnvironmentSetup) -> Result<(), ()> {
    let cluster_setup_wait = async_process!(async {
        if option.remove_cluster_before() {
            println!("Deleting existing cluster before starting test");
            let mut setup = TestCluster::new(option.clone());
            setup.remove_cluster().await;
        }

        if option.cluster_start() || option.remove_cluster_before() {
            println!("Starting cluster and testing connection");
            let mut test_cluster = TestCluster::new(option.clone());

            test_cluster
                .start()
                .await
                .expect("Unable to connect to fresh test cluster");
        } else {
            println!("Testing connection to Fluvio cluster in profile");
            Fluvio::connect()
                .await
                .expect("Unable to connect to Fluvio test cluster via profile");
        }
    });

    let _ = cluster_setup_wait
        .join()
        .expect("Cluster setup wait failed");

    Ok(())
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
