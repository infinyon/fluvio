#![allow(irrefutable_let_patterns)]

use std::sync::Arc;
use structopt::StructOpt;
use fluvio::Fluvio;
use fluvio_test_util::test_meta::{TestCase, CliArgs, TestCli};
use fluvio_test_util::setup::TestCluster;
use fluvio_future::task::run_block_on;

const TESTS: &[&str] = &["smoke","concurrent","many_producers"];
fn main() {
    run_block_on(async {
        //let option: TestCase = CliArgs::from_args().into();
        let option = CliArgs::from_args();
        println!("{:?}", option);

        // Verify if we have a test by name or print help

        let test_name;
        let test_opts;

        if let TestCli::CliCmd(cmd) = option.test_cmd {
            if TESTS.iter().any(|t| &cmd[0].as_str() == t) {
                println!("Command is: {:?}", cmd[0]);
                println!("options are: {:?}", &cmd[1..]);

                test_name = cmd[0].clone();
                test_opts = cmd[1..].to_vec();
            } else {
                //CliArgs::clap().print_help().expect("print help");
                eprintln!("Tests: {:?}", TESTS);
                println!();
                return
            }
        }

        // catch panic in the spawn
        std::panic::set_hook(Box::new(|panic_info| {
            eprintln!("panic {}", panic_info);
            std::process::exit(-1);
        }));

        println!("Start running fluvio test runner");
        fluvio_future::subscriber::init_logger();

        //// Deploy a cluster
        //let fluvio_client = cluster_setup(&option).await;

        //// TODO: Build this with Test Runner
        //match option.name.as_str() {
        //    "smoke" => flv_test::tests::smoke::run(fluvio_client, option.clone()).await,
        //    "concurrent" => flv_test::tests::concurrent::run(fluvio_client, option.clone()).await,
        //    "many_producers" => {
        //        flv_test::tests::many_producers::run(fluvio_client, option.clone()).await
        //    }
        //    _ => panic!("Test not found"),
        //};

        //// Cluster cleanup
        //cluster_cleanup(option).await;
    });
}

async fn cluster_cleanup(option: TestCase) {
    if option.skip_cluster_delete() {
        println!("skipping cluster delete");
    } else {
        let mut setup = TestCluster::new(option.clone());
        setup.remove_cluster().await;
    }
}

async fn cluster_setup(option: &TestCase) -> Arc<Fluvio> {
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
    use fluvio_test_util::test_meta::{CliArgs, TestCase};
    use std::collections::HashMap;

    #[test]
    fn valid_test_name() {
        let args = CliArgs::from_iter(vec!["flv-test", "smoke"]);
        assert_eq!(args.test_name, String::from("smoke"));
    }

    // TODO: Add a test selector, so we can make this test more robust
    #[test]
    fn invalid_test_name() {
        let args = CliArgs::from_iter(vec!["flv-test", "testdoesnotexist"]);
        assert_eq!(args.test_name, String::from("testdoesnotexist"));
    }

    #[test]
    fn extra_vars() {
        let args = CliArgs::from_iter(vec![
            "flv-test",
            "smoke",
            "--var",
            "producer.iteration=9000",
            "--var",
            "producer.record_size=1000",
        ]);
        let test_case: TestCase = args.into();

        let mut expected_hashmap = HashMap::new();
        expected_hashmap.insert("producer.iteration".to_string(), "9000".to_string());
        expected_hashmap.insert("producer.record_size".to_string(), "1000".to_string());

        assert_eq!(test_case.vars, expected_hashmap);
    }

    #[test]
    fn topic() {
        let args = CliArgs::from_iter(vec![
            "flv-test",
            "smoke",
            "--topic-name",
            "not_the_default_topic_name",
        ]);
        let test_case: TestCase = args.into();

        assert_eq!(
            test_case.environment.topic_name,
            "not_the_default_topic_name"
        );
    }

    #[test]
    fn spu() {
        let args = CliArgs::from_iter(vec!["flv-test", "smoke", "--spu", "5"]);
        let test_case: TestCase = args.into();

        assert_eq!(test_case.environment.spu, 5);
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
