use std::collections::HashMap;
use std::panic::AssertUnwindSafe;
use std::process::{exit, self};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread;
use std::time::{SystemTime, Duration};

use indicatif::{ProgressBar, ProgressStyle};
use clap::Parser;

use fluvio::Fluvio;
use fluvio_test_util::test_meta::{BaseCli, TestCase, TestCli, TestOption};
use fluvio_test_util::test_meta::test_result::TestResult;
use fluvio_test_util::test_meta::environment::{EnvDetail, EnvironmentSetup};
use fluvio_test_util::setup::TestCluster;
use fluvio_test_util::test_runner::test_driver::TestDriver;
use fluvio_test_util::test_runner::test_meta::FluvioTestMeta;
use fluvio_test_util::async_process;

// This is important for `inventory` crate
#[allow(unused_imports)]
use fluvio_test::tests as _;
use sysinfo::{System, get_current_pid, Signal, Process, Pid};
use tracing::debug;

//const CI_FAIL_FLAG: &str = "/tmp/CI_FLUVIO_TEST_FAIL";

fn main() {
    let option = BaseCli::parse();

    debug!("{:?}", option);

    let test_name = option.environment.test_name.clone();

    // Get test from inventory
    let test_meta = FluvioTestMeta::from_name(test_name.clone())
        .expect("StructOpt should have caught this error");

    let mut subcommand = vec![test_name];

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

    let test_result = run_test(option.environment.clone(), test_opt, test_meta);

    cluster_cleanup(option.environment);
    println!("{test_result}");

    if test_result.success {
        exit(0)
    } else {
        exit(-1)
    }
}

fn run_test(
    environment: EnvironmentSetup,
    test_opt: Box<dyn TestOption>,
    test_meta: &FluvioTestMeta,
) -> TestResult {
    let start = SystemTime::now();
    let test_case = TestCase::new(environment.clone(), test_opt);
    let test_cluster_opts = TestCluster::new(environment.clone());
    let test_driver = TestDriver::new(Some(test_cluster_opts));

    let finished_signal = Arc::new(AtomicBool::new(false));
    let fail_signal = Arc::new(AtomicBool::new(false));
    signal_hook::flag::register(signal_hook::consts::SIGUSR1, Arc::clone(&finished_signal))
        .expect("fail to register ok signal hook");
    signal_hook::flag::register(signal_hook::consts::SIGUSR2, Arc::clone(&fail_signal))
        .expect("fail to register fail signal hook");

    // println!("supported signals: {:?}", System::SUPPORTED_SIGNALS);
    let root_pid = get_current_pid().expect("Unable to get current pid");
    debug!(?root_pid, "current root pid");
    sysinfo::set_open_files_limit(0);
    let mut sys = System::new();
    sys.refresh_processes(sysinfo::ProcessesToUpdate::Some(&[root_pid]));
    let root_process = sys.process(root_pid).expect("Unable to get root process");
    let _child_pid = match fork::fork() {
        Ok(fork::Fork::Parent(child_pid)) => child_pid,
        Ok(fork::Fork::Child) => {
            println!("starting test in child process");
            // put panic handler, this shows proper stack trace in the console unlike hook
            let status = std::panic::catch_unwind(AssertUnwindSafe(|| {
                (test_meta.test_fn)(test_driver, test_case)
            }));
            let parent_id = get_parent_pid();
            debug!(
                "catch_unwind. PID {:?}, root_pid {root_pid}, parent_id {parent_id}",
                get_current_pid().unwrap(),
            );
            if parent_id == root_pid {
                println!("test complete, signaling to parent");
                root_process.kill_with(Signal::User1);
            }
            if let Err(err) = status {
                if environment.expect_fail {
                    println!("test failed as expected, signaling parent");
                } else {
                    println!("test failed {err:#?}, signaling parent");
                }
                // This doesn't actually kill root_process, just sends it the signal
                root_process.kill_with(Signal::User2);
                process::exit(1);
            } else {
                process::exit(0);
            }
        }
        Err(_) => panic!("Fork failed"),
    };

    debug!("running waiting for signal");

    const TICK: Duration = Duration::from_millis(100);
    let disable_timeout = environment.disable_timeout;
    let timeout = environment.timeout();
    let mut message_spinner = create_spinning_indicator();
    let mut timed_out = false;

    loop {
        if finished_signal.load(Ordering::Relaxed) {
            debug!("signal received");
            break;
        }

        let elapsed = start.elapsed().expect("Unable to get elapsed time");
        if !disable_timeout && elapsed > timeout {
            timed_out = true;
            break;
        }
        thread::sleep(TICK);
        if let Some(pb) = &mut message_spinner {
            pb.set_message(format!("waiting for test {} seconds", elapsed.as_secs()));
        }
    }

    let success = fail_signal.load(Ordering::Relaxed) == environment.expect_fail;

    debug!(success, "signal status");

    if timed_out {
        let success = if environment.expect_timeout {
            println!(
                "Test timed out as expected after {} seconds",
                timeout.as_secs()
            );
            true
        } else {
            println!("Test timed out after {} seconds", timeout.as_secs());
            false
        };
        kill_child_processes(root_process);
        TestResult {
            success,
            duration: start.elapsed().unwrap(),
            ..std::default::Default::default()
        }
    } else if success {
        println!("Test passed");
        TestResult {
            success: true,
            duration: start.elapsed().unwrap(),
            ..std::default::Default::default()
        }
    } else {
        println!("Test failed");
        TestResult {
            success: false,
            duration: start.elapsed().unwrap(),
            ..std::default::Default::default()
        }
    }
}

/// kill all children of the root processes
fn kill_child_processes(root_process: &Process) {
    let root_pid = root_process.pid();
    sysinfo::set_open_files_limit(0);
    let mut sys2 = System::new();
    sys2.refresh_processes(sysinfo::ProcessesToUpdate::All);
    let g_id = root_process.group_id();

    let processes = sys2.processes();

    fn is_root(process: &Process, r_id: Pid, processes: &HashMap<Pid, Process>) -> bool {
        if let Some(parent_id) = process.parent() {
            if parent_id == r_id {
                true
            } else if let Some(parent_process) = processes.get(&parent_id) {
                is_root(parent_process, r_id, processes)
            } else {
                false
            }
        } else {
            false
        }
    }

    for (pid, process) in processes {
        if pid != &root_pid && process.group_id() == g_id && is_root(process, root_pid, processes) {
            println!("killing child test pid {} name {:?}", pid, process.name());
            process.kill();
        }
    }
}

fn cluster_cleanup(option: EnvironmentSetup) {
    if option.cluster_delete() {
        let mut setup = TestCluster::new(option);

        let cluster_cleanup_wait = async_process!(
            async {
                setup.remove_cluster().await;
            },
            "cluster_cleanup"
        );
        cluster_cleanup_wait
            .join()
            .expect("Cluster cleanup wait failed");
    }
}

// FIXME: Need to confirm SPU options count match cluster. Offer self-correcting behavior
fn cluster_setup(option: &EnvironmentSetup) -> Result<(), ()> {
    let cluster_setup_wait = async_process!(
        async {
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
        },
        "cluster setup"
    );

    cluster_setup_wait
        .join()
        .expect("Cluster setup wait failed");

    Ok(())
}

fn create_spinning_indicator() -> Option<ProgressBar> {
    if std::env::var("CI").is_ok() {
        None
    } else {
        let pb = ProgressBar::new(1);
        pb.set_style(
            ProgressStyle::default_bar()
                .template("{msg} {spinner}")
                .expect("Unable to set template content")
                .tick_chars("/-\\|"),
        );
        pb.enable_steady_tick(Duration::from_millis(100));
        Some(pb)
    }
}

fn get_parent_pid() -> sysinfo::Pid {
    let pid = get_current_pid().expect("Unable to get current pid");
    sysinfo::set_open_files_limit(0);
    let mut sys2 = System::new();
    sys2.refresh_processes(sysinfo::ProcessesToUpdate::All);
    let current_process = sys2.process(pid).expect("Current process not found");
    current_process.parent().expect("Parent process not found")
}

#[cfg(test)]
mod tests {
    // CLI Tests

    use std::time::Duration;
    use clap::Parser;
    use fluvio_test_util::test_meta::{BaseCli, TestCli};
    use fluvio_test_util::test_meta::environment::EnvDetail;
    use fluvio_test::tests::smoke::SmokeTestOption;

    #[test]
    fn valid_test_name() {
        let args = BaseCli::try_parse_from(vec!["fluvio-test", "smoke"]);

        assert!(args.is_ok());
    }

    #[test]
    fn invalid_test_name() {
        let args = BaseCli::try_parse_from(vec!["fluvio-test", "testdoesnotexist"]);

        assert!(args.is_err());
    }

    #[test]
    fn extra_vars() {
        let args = BaseCli::parse_from(vec![
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

            let smoke_test_case = SmokeTestOption::parse_from(subcommand);

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
        let args = BaseCli::parse_from(vec![
            "fluvio-test",
            "smoke",
            "--topic-name",
            "not_the_default_topic_name",
        ]);

        assert_eq!(
            args.environment.base_topic_name(),
            "not_the_default_topic_name"
        );
    }

    #[test]
    fn spu() {
        let args = BaseCli::parse_from(vec!["fluvio-test", "smoke", "--spu", "5"]);

        assert_eq!(args.environment.spu, 5);
    }

    #[test]
    fn timeout() {
        let args = BaseCli::parse_from(vec!["fluvio-test", "smoke", "--timeout", "9000s"]);

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
    //        let skip_cluster_delete_cmd = CliArgs::parse_from(vec![
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

    //        let skip_cluster_start_cmd = CliArgs::parse_from(vec![
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
