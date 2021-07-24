use std::sync::Arc;
use std::process::exit;
use std::time::Duration;
use structopt::StructOpt;
use fluvio::Fluvio;
use fluvio_test_util::test_meta::{BaseCli, TestCase, TestCli, TestOption};
use fluvio_test_util::test_meta::test_result::TestResult;
use fluvio_test_util::test_meta::environment::{EnvDetail, EnvironmentSetup};
use fluvio_test_util::setup::TestCluster;
use fluvio_future::task::{run_block_on, spawn};
use std::panic::{self, AssertUnwindSafe};
use fluvio_test_util::test_runner::test_driver::{FluvioTestDriver, TestDriverType};
use fluvio_test_util::test_runner::test_meta::FluvioTestMeta;
use fluvio_test_util::test_meta::test_timer::TestTimer;
use fluvio_test_util::test_meta::chart_builder::{ChartBuilder, FluvioTimeData};
use fluvio_test_util::test_meta::data_export::DataExporter;
use fluvio_future::timer::sleep;
use async_channel::TryRecvError;
use sysinfo::{System, SystemExt};

// # of nanoseconds in a millisecond
const NANOS_IN_MILLIS: f32 = 1_000_000.0;

// This is important for `inventory` crate
#[allow(unused_imports)]
use flv_test::tests as _;
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
        let test_opt: Box<dyn TestOption> =
            if let Some(TestCli::Args(args)) = option.test_cmd_args.clone() {
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
        let fluvio_client = cluster_setup(&option).await;

        // Check on test requirements before running the test
        if &option.runner_opts.cluster_type == "fluvio" {
            // Check on test requirements before running the test
            if !FluvioTestDriver::is_env_acceptable(
                &(test_meta.requirements)(),
                &TestCase::new(option.environment.clone(), test_opt.clone()),
            ) {
                exit(-1);
            }
        }

        let _panic_timer = TestTimer::new().start();
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
            if let Some(s) = panic_info.payload().downcast_ref::<&str>() {
                eprintln!("{:?}", s);
            } else {
                eprintln!("There was no string error message provided in panic.");
            }

            if let Some(l) = panic_info.location() {
                eprintln!("file: {} @ line {}", l.file(), l.line());
            } else {
                eprintln!("There was no location information from panic.");
            }
        }));
        */

        let system_profiler_client = fluvio_client.clone();
        let (s, r) = async_channel::unbounded();
        spawn(async move {
            let mut s = System::new_all();

            let start_mem_used = s.used_memory() as f32 / 1_000.0_f32;

            loop {
                match r.try_recv() {
                    Err(TryRecvError::Empty) => {
                        // Do a sample here

                        // Do this at a process-level?

                        s.refresh_cpu();
                        let cpu_used = s.load_average().one * 100.0;
                        s.refresh_memory();
                        let mem_used = (s.used_memory() as f32 / 1_000.0_f32) - start_mem_used;

                        let mut lock = system_profiler_client.write().await;
                        let timestamp = lock.test_elapsed().as_nanos() as f32 / NANOS_IN_MILLIS;
                        //println!("{} MB", mem_used);

                        lock.memory_usage += mem_used as u64;
                        lock.memory_time_usage.push(FluvioTimeData {
                            test_elapsed_ms: timestamp,
                            data: mem_used as f32,
                        });

                        lock.cpu_usage += cpu_used as u64;
                        lock.cpu_time_usage.push(FluvioTimeData {
                            test_elapsed_ms: timestamp,
                            data: cpu_used as f32,
                        });
                        drop(lock);
                    }
                    _ => {
                        let _ = r.close();
                    }
                }

                sleep(Duration::from_secs(1));
            }
        });

        let test_result = run_test(
            option.environment.clone(),
            test_opt,
            test_meta,
            fluvio_client.clone(),
            s,
        )
        .await;
        cluster_cleanup(option.environment).await;

        if let Ok(t) = test_result {
            println!("{}", t);

            if !option.runner_opts.skip_data_save {
                // TODO: Make sure the target directory exists

                // Producer Latency timeseries
                ChartBuilder::latency_x_time(
                    t.producer_time_latency.clone(),
                    t.producer_latency_histogram.clone(),
                    "(Unofficial) Producer Latency x Time",
                    "Producer Latency (ms)",
                    "Test duration (ms)",
                    &format!(
                        "{}/producer-latency-x-time.svg",
                        option.runner_opts.results_dir.display()
                    ),
                );

                // Consumer Latency timeseries
                ChartBuilder::latency_x_time(
                    t.consumer_time_latency.clone(),
                    t.consumer_latency_histogram.clone(),
                    "(Unofficial) Consumer Latency x Time",
                    "Consumer Latency (ms)",
                    "Test duration (ms)",
                    &format!(
                        "{}/consumer-latency-x-time.svg",
                        option.runner_opts.results_dir.display()
                    ),
                );

                // E2E Latency timeseries
                ChartBuilder::latency_x_time(
                    t.e2e_time_latency.clone(),
                    t.e2e_latency_histogram.clone(),
                    "(Unofficial) End-to-end Latency x Time",
                    "E2E Latency (ms)",
                    "Test duration (ms)",
                    &format!(
                        "{}/e2e-latency-x-time.svg",
                        option.runner_opts.results_dir.display()
                    ),
                );

                // Producer latency percentile chart
                ChartBuilder::latency_x_percentile(
                    t.producer_latency_histogram.clone(),
                    "(Unofficial) Producer Latency x Percentile",
                    "Producer Latency (ms)",
                    "Percentile (%)",
                    &format!(
                        "{}/producer-latency-x-percentile.svg",
                        option.runner_opts.results_dir.display()
                    ),
                );

                // Consumer latency percentile chart
                ChartBuilder::latency_x_percentile(
                    t.consumer_latency_histogram.clone(),
                    "(Unofficial) Consumer Latency x Percentile",
                    "Consumer Latency (ms)",
                    "Percentile (%)",
                    &format!(
                        "{}/consumer-latency-x-percentile.svg",
                        option.runner_opts.results_dir.display()
                    ),
                );

                // E2E latency percentile chart
                ChartBuilder::latency_x_percentile(
                    t.e2e_latency_histogram.clone(),
                    "(Unofficial) End-to-end Latency x Percentile",
                    "E2E Latency (ms)",
                    "Percentile (%)",
                    &format!(
                        "{}/e2e-latency-x-percentile.svg",
                        option.runner_opts.results_dir.display()
                    ),
                );

                // Producer Throughput timeseries
                ChartBuilder::data_x_time(
                    t.producer_time_rate.clone(),
                    t.producer_rate_histogram.clone(),
                    "(Unofficial) Producer Data x Time",
                    "Producer sent (Kbytes)",
                    "Test duration (ms)",
                    &format!(
                        "{}/producer-data-x-time.svg",
                        option.runner_opts.results_dir.display()
                    ),
                );

                // Consumer Throughput timeseries
                ChartBuilder::data_x_time(
                    t.consumer_time_rate.clone(),
                    t.consumer_rate_histogram.clone(),
                    "(Unofficial) Consumer Data x Time",
                    "Consumer received (Kbytes)",
                    "Test duration (ms)",
                    &format!(
                        "{}/consumer-data-x-time.svg",
                        option.runner_opts.results_dir.display()
                    ),
                );

                // Memory usage timeseries
                ChartBuilder::mem_usage_x_time(
                    t.memory_time_usage.clone(),
                    t.memory_usage_histogram.clone(),
                    "(Unofficial) Memory usage x Time",
                    "Memory in use (Mbytes)",
                    "Test duration (ms)",
                    &format!(
                        "{}/memory-usage-x-time.svg",
                        option.runner_opts.results_dir.display()
                    ),
                );

                // CPU usage timeseries
                ChartBuilder::cpu_usage_x_time(
                    t.cpu_time_usage.clone(),
                    t.cpu_usage_histogram.clone(),
                    "(Unofficial) CPU usage x Time",
                    "CPU use (%)",
                    "Test duration (ms)",
                    &format!(
                        "{}/cpu-usage-x-time.svg",
                        option.runner_opts.results_dir.display()
                    ),
                );

                DataExporter::timeseries_as_csv(
                    t.producer_time_latency.clone(),
                    &format!(
                        "{}/producer-latency-x-time.csv",
                        option.runner_opts.results_dir.display()
                    ),
                );
                DataExporter::timeseries_as_csv(
                    t.consumer_time_latency.clone(),
                    &format!(
                        "{}/consumer-latency-x-time.csv",
                        option.runner_opts.results_dir.display()
                    ),
                );
                DataExporter::timeseries_as_csv(
                    t.e2e_time_latency.clone(),
                    &format!(
                        "{}/e2e-latency-x-time.csv",
                        option.runner_opts.results_dir.display()
                    ),
                );
                DataExporter::timeseries_as_csv(
                    t.producer_time_rate.clone(),
                    &format!(
                        "{}/producer-data-x-time.csv",
                        option.runner_opts.results_dir.display()
                    ),
                );
                DataExporter::timeseries_as_csv(
                    t.consumer_time_rate.clone(),
                    &format!(
                        "{}/consumer-data-x-time.csv",
                        option.runner_opts.results_dir.display()
                    ),
                );
                DataExporter::timeseries_as_csv(
                    t.memory_time_usage.clone(),
                    &format!(
                        "{}/memory-usage-x-time.csv",
                        option.runner_opts.results_dir.display()
                    ),
                );
                DataExporter::timeseries_as_csv(
                    t.cpu_time_usage.clone(),
                    &format!(
                        "{}/cpu-usage-x-time.csv",
                        option.runner_opts.results_dir.display()
                    ),
                );

                DataExporter::percentile_as_csv(
                    t.producer_latency_histogram.clone(),
                    &format!(
                        "{}/producer-latency-percentile.csv",
                        option.runner_opts.results_dir.display()
                    ),
                );

                DataExporter::percentile_as_csv(
                    t.consumer_latency_histogram.clone(),
                    &format!(
                        "{}/consumer-latency-percentile.csv",
                        option.runner_opts.results_dir.display()
                    ),
                );

                DataExporter::percentile_as_csv(
                    t.consumer_latency_histogram,
                    &format!(
                        "{}/e2e-latency-percentile.csv",
                        option.runner_opts.results_dir.display()
                    ),
                );
            }
        }
    });
}

async fn run_test(
    environment: EnvironmentSetup,
    test_opt: Box<dyn TestOption>,
    test_meta: &FluvioTestMeta,
    test_driver: Arc<RwLock<FluvioTestDriver>>,
    system_profiler_channel: async_channel::Sender<()>,
    // Add channel
) -> Result<TestResult, TestResult> {
    let test_case = TestCase::new(environment, test_opt);
    let test_result = panic::catch_unwind(AssertUnwindSafe(move || {
        (test_meta.test_fn)(test_driver, test_case)
    }));

    // Close channel to stop system utilization polling
    system_profiler_channel.close();

    if let Ok(t) = test_result {
        t
    } else {
        Err(TestResult::default())
    }
}

async fn cluster_cleanup(option: EnvironmentSetup) {
    if option.skip_cluster_delete() {
        println!("skipping cluster delete\n");
    } else {
        let mut setup = TestCluster::new(option.clone());
        setup.remove_cluster().await;
    }
}

async fn cluster_setup(option: &BaseCli) -> Arc<RwLock<FluvioTestDriver>> {
    // TODO: Maybe have an enum for the types of cluster drivers we support
    match option.runner_opts.cluster_type.as_str() {
        "fluvio" => {
            let env = &option.environment;
            let fluvio_client = if env.skip_cluster_start() {
                println!("skipping cluster start");
                // Connect to cluster in profile
                Arc::new(TestDriverType::Fluvio(
                    Fluvio::connect()
                        .await
                        .expect("Unable to connect to Fluvio test cluster via profile"),
                ))
            } else {
                let mut test_cluster = TestCluster::new(env.clone());
                Arc::new(TestDriverType::Fluvio(
                    test_cluster
                        .start()
                        .await
                        .expect("Unable to connect to fresh test cluster"),
                ))
            };

            Arc::new(RwLock::new(
                FluvioTestDriver::new(fluvio_client)
                    .with_cluster_addr(option.environment.cluster_addr.clone())
                    .with_producer_batch_ms(option.environment.batch_ms)
                    .with_producer_batch_size(option.environment.batch_kbytes)
                    .with_producer_record_size(option.environment.record_bytes),
            ))
        }
        "pulsar" => Arc::new(RwLock::new(
            FluvioTestDriver::new(Arc::new(TestDriverType::Pulsar))
                .with_cluster_addr(option.environment.cluster_addr.clone())
                .with_producer_batch_ms(option.environment.batch_ms)
                .with_producer_batch_size(option.environment.batch_kbytes)
                .with_producer_record_size(option.environment.record_bytes),
        )),
        "kafka" => Arc::new(RwLock::new(
            FluvioTestDriver::new(Arc::new(TestDriverType::Kafka))
                .with_cluster_addr(option.environment.cluster_addr.clone())
                .with_producer_batch_ms(option.environment.batch_ms)
                .with_producer_batch_size(option.environment.batch_kbytes)
                .with_producer_record_size(option.environment.record_bytes),
        )),
        _ => unreachable!(),
    }
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
