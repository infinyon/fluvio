use std::sync::Arc;
use structopt::StructOpt;
use fluvio::Fluvio;
use fluvio_test_util::test_meta::{BaseCli, TestCase, TestCli, TestOption};
use fluvio_test_util::test_meta::environment::{EnvDetail, EnvironmentSetup};
use fluvio_test_util::setup::TestCluster;
use fluvio_future::task::run_block_on;
use std::panic::{self, AssertUnwindSafe};
use fluvio_test_util::test_runner::FluvioTest;

// This is important for `inventory` crate
#[allow(unused_imports)]
use flv_test::tests as _;

fn main() {
    run_block_on(async {
        let option = BaseCli::from_args();
        //println!("{:?}", option);

        let test_name = option.test_name.clone();
        let mut subcommand = vec![test_name.clone()];

        // We want to get a TestOption compatible struct back
        let valid_cmd: Result<Box<dyn TestOption>, ()> =
            if let Some(TestCli::Args(args)) = option.test_cmd_args {
                // Add the args to the subcommand
                subcommand.extend(args);

                // Find test in inventory
                let t = inventory::iter::<FluvioTest>
                    .into_iter()
                    .find(|t| t.name == test_name.as_str())
                    .expect("Test not found");

                // Parse the subcommand
                let testopt = (t.validate_fn)(subcommand);
                Ok(testopt)
            } else {
                // No args
                let t = inventory::iter::<FluvioTest>
                    .into_iter()
                    .find(|t| t.name == test_name.as_str())
                    .expect("Test not found");

                let testopt = (t.validate_fn)(subcommand);
                Ok(testopt)
            };

        let test_opt = if let Ok(test_opt) = valid_cmd {
            test_opt
        } else {
            eprintln!(
                "Tests: {:?}",
                inventory::iter::<FluvioTest>
                    .into_iter()
                    .map(|x| x.name.clone())
                    .collect::<Vec<String>>()
            );
            eprintln!();

            return;
        };

        // catch panic in the spawn
        std::panic::set_hook(Box::new(|panic_info| {
            eprintln!("panic {}", panic_info);
            std::process::exit(-1);
        }));

        println!("Start running fluvio test runner");
        fluvio_future::subscriber::init_logger();

        // Deploy a cluster
        let fluvio_client = cluster_setup(&option.environment).await;

        // Create a TestCase object with option.envronment and test_opt
        let test_case = TestCase::new(option.environment.clone(), test_opt);

        let test_run = panic::catch_unwind(AssertUnwindSafe(move || {
            let test = inventory::iter::<FluvioTest>
                .into_iter()
                .find(|t| t.name == test_name.as_str())
                .expect("Test not found");

            // Run the test
            let test_result = (test.test_fn)(fluvio_client.clone(), test_case);
            println!("{}", test_result);
        }));
        // Cluster cleanup
        cluster_cleanup(option.environment).await;

        if let Err(err) = test_run {
            eprintln!("panic reason: {:?}", err);
            std::process::exit(-1);
        }
        //}));
    });
}

async fn cluster_cleanup(option: EnvironmentSetup) {
    if option.skip_cluster_delete() {
        println!("skipping cluster delete");
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

    // Extra vars
    // No test name
    // Invalid test name
    // skip cluster delete then skip cluster start
    // set topic name
    // set # spu

    use structopt::StructOpt;
    use fluvio_test_util::test_meta::{BaseCli, TestCli};
    use fluvio_test_util::test_runner::FluvioTest;
    use flv_test::tests::smoke::SmokeTestOption;

    #[test]
    fn valid_test_name() {
        let args = BaseCli::from_iter(vec!["flv-test", "smoke"]);

        let t = inventory::iter::<FluvioTest>
            .into_iter()
            .find(|t| t.name == args.test_name.as_str());

        assert!(t.is_some());
    }

    #[test]
    fn invalid_test_name() {
        let args = BaseCli::from_iter(vec!["flv-test", "testdoesnotexist"]);

        let t = inventory::iter::<FluvioTest>
            .into_iter()
            .find(|t| t.name == args.test_name.as_str());

        assert!(t.is_none());
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
            let mut subcommand = vec![args.test_name.clone()];
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
