use proc_macro::TokenStream;
use proc_macro2::Span;

use fluvio_test_util::test_meta::derive_attr::TestRequirements;
use syn::{AttributeArgs, Ident, ItemFn, parse_macro_input};
use quote::quote;
use inflections::Inflect;

/// This macro will allow a test writer to override
/// minimum Fluvio cluster requirements for a test
///
/// Supported keys:
/// * `min_spu` (default `1`)
/// * `topic_name` (default: `topic`)
/// * `timeout` (default: `3600` seconds)
/// * `cluster_type` (default: no cluster restrictions)
/// * `name` (default: uses function name)
///
/// #[fluvio_test(min_spu = 2)]
/// pub async fn run(client: Arc<Fluvio>, mut test_case: TestCase) {
///     println!("Test")
/// }
///
/// Will expand into the following pseudocode
///
/// pub async fn run(client: Fluvio, option: TestCase) {
///     1. Read in TestRequirements from macro attrs
///     2. Compare TestRequirements with the current TestCase
///         - Any test that doesn't meet explicit requirements, skip to 6
///     3. Customize environment per TestRequirements
///     4. Create a topic
///     5. Start a timed test with (a default 1 hour timeout)
///     6. Return a TestResult
/// }
#[proc_macro_attribute]
pub fn fluvio_test(args: TokenStream, input: TokenStream) -> TokenStream {
    let fn_test_reqs: TestRequirements =
        match TestRequirements::from_ast(parse_macro_input!(args as AttributeArgs)) {
            Ok(attr) => attr,
            Err(_err) => panic!("Parse failed"),
        };

    // Serializing to string to pass into quote! block
    let fn_test_reqs_str =
        serde_json::to_string(&fn_test_reqs).expect("Could not serialize test reqs");

    // Read the user test
    let fn_user_test = parse_macro_input!(input as ItemFn);

    // If test name is given, then use that instead of the test's function name
    let out_fn_iden = if let Some(req_test_name) = &fn_test_reqs.test_name {
        Ident::new(&req_test_name.to_string(), Span::call_site())
    } else {
        fn_user_test.sig.ident
    };

    let test_name = out_fn_iden.to_string();

    // Enforce naming convention for converting dyn TestOption to concrete type
    let test_opt_ident = Ident::new(
        &format!("{}TestOption", &test_name).to_pascal_case(),
        Span::call_site(),
    );

    // Finally, the test body
    let test_body = &fn_user_test.block;

    let output_fn = quote! {

        pub fn validate_subcommand(subcmd: Vec<String>) -> Box<dyn TestOption> {
            Box::new(#test_opt_ident::from_iter(subcmd))
        }

        pub fn requirements() -> TestRequirements {
            serde_json::from_str(#fn_test_reqs_str).expect("Could not deserialize test reqs")
        }

        inventory::submit!{
            FluvioTestMeta {
                name: #test_name.to_string(),
                test_fn: #out_fn_iden,
                validate_fn: validate_subcommand,
                requirements: requirements,
            }
        }

        #[allow(clippy::unnecessary_operation)]
        pub fn ext_test_fn(mut test_driver: TestDriver, test_case: TestCase) {
            use fluvio_test_util::test_meta::environment::EnvDetail;
            #test_body;
        }

        pub fn #out_fn_iden(mut test_driver: TestDriver, mut test_case: TestCase) -> Result<TestResult, TestResult> {
            use fluvio::Fluvio;
            use fluvio_test_util::test_meta::TestCase;
            use fluvio_test_util::test_meta::test_result::TestResult;
            use fluvio_test_util::test_meta::environment::{EnvDetail};
            use fluvio_test_util::test_meta::derive_attr::TestRequirements;
            use fluvio_test_util::test_meta::test_timer::TestTimer;
            use fluvio_test_util::test_runner::test_driver::TestDriver;
            use fluvio_test_util::test_runner::test_meta::FluvioTestMeta;
            use fluvio_test_util::setup::environment::EnvironmentType;
            use fluvio_future::task::run;
            use fluvio_future::timer::sleep;
            use std::{io, time::Duration};
            use std::panic::panic_any;
            use std::default::Default;
            use fork::{fork, Fork};
            use nix::sys::wait::waitpid;
            use nix::sys::signal::{kill, Signal};
            use nix::unistd::Pid;
            use std::thread;
            use std::process::exit;
            use tracing::debug;
            use crossbeam_channel::{Select, unbounded};

            let test_reqs : TestRequirements = serde_json::from_str(#fn_test_reqs_str).expect("Could not deserialize test reqs");
            //let test_reqs : TestRequirements = #fn_test_reqs;

            let is_env_acceptable = TestDriver::is_env_acceptable(&test_reqs, &test_case);

            // Customize test environment if it meets minimum requirements
            if is_env_acceptable {

                // Test-level environment customizations from macro attrs
                FluvioTestMeta::customize_test(&test_reqs, &mut test_case);

                // Setup topics before starting test
                // Doing setup in another process to avoid async in parent process
                // Otherwise there is .await blocking in child processes if tests fork() too
                let topic_setup_process = match fork() {
                    Ok(Fork::Parent(child_pid)) => child_pid,
                    Ok(Fork::Child) => {
                        run_block_on(async {
                            let mut test_driver_setup = test_driver.clone();
                            // Connect test driver to cluster before starting test
                            test_driver_setup.connect().await.expect("Unable to connect to cluster");

                            // Create topic before starting test
                            test_driver_setup.create_topic(&test_case.environment)
                                .await
                                .expect("Unable to create default topic");

                            // Disconnect test driver to cluster before starting test
                            test_driver_setup.disconnect();
                        });

                        exit(0);
                    }
                    Err(_) => panic!("Topic setup fork failed"),
                };

                let topic_setup_wait = thread::spawn( move || {
                    let pid = Pid::from_raw(topic_setup_process);
                    match waitpid(pid, None) {
                        Ok(status) => {
                            debug!("[main] Topic setup exited with status {:?}", status);
                        }
                        Err(err) => panic!("[main] Topic setup failed: {}", err),
                    }
                });
                let _ = topic_setup_wait.join().expect("Topic setup wait failed");

                 // Start a test timer for the user's test now that setup is done
                let mut test_timer = TestTimer::start();
                let (test_end, test_end_listener) = unbounded();

                let test_process = match fork() {
                    Ok(Fork::Parent(child_pid)) => child_pid,
                    Ok(Fork::Child) => {
                        ext_test_fn(test_driver, test_case);
                        exit(0);
                    }
                    Err(_) => panic!("Test fork failed"),
                };

                let test_wait = thread::spawn( move || {
                    let pid = Pid::from_raw(test_process.clone());
                    match waitpid(pid, None) {
                        Ok(status) => {
                            debug!("[main] Test exited with status {:?}", status);

                            // Send something through the channel to signal test completion
                            test_end.send(()).unwrap();
                        }
                        Err(err) => panic!("[main] Test failed: {}", err),
                    }
                });

                // All of this is to handle test timeout
                let mut sel = Select::new();
                let test_thread_done = sel.recv(&test_end_listener);
                let thread_selector = sel.select_timeout(test_case.environment.timeout());

                match thread_selector {
                    Err(_) => {
                        // Need to catch this panic to kill test parent process + all child processes
                        panic!("Test timeout met: {:?}", test_case.environment.timeout());
                    },
                    Ok(thread_selector) => match thread_selector.index() {
                        i if i == test_thread_done => {
                            // This is needed to let crossbeam select channel know we selected an operation, or it'll panic
                            let _ = thread_selector.recv(&test_end_listener);

                            // Stop the test timer before reporting
                            test_timer.stop();

                            Ok(TestResult {
                                success: true,
                                duration: test_timer.duration(),
                                ..Default::default()
                            })


                        }
                        _ => unreachable!()
                    }
                }
            } else {
                println!("Test skipped...");

                Ok(TestResult {
                    success: true,
                    duration: Duration::new(0, 0),
                    ..Default::default()
                })
            }
        }

    };

    output_fn.into()
}
