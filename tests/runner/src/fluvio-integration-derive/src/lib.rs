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

    // We're going to wrap the the async test into a sync to store fn pointer w/ `inventory` crate
    let async_inner_fn_iden = Ident::new(&format!("{}_inner", &test_name), Span::call_site());

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

        pub fn #out_fn_iden(client: Arc<Fluvio>, mut test_case: TestCase) -> Result<TestResult, TestResult> {
            //println!("Inside the function");
            let future = async move {
                //println!("Inside the async wrapper function");
                #async_inner_fn_iden(client, test_case).await
            };
            fluvio_future::task::run_block_on(future)
        }

        inventory::submit!{
            FluvioTest {
                name: #test_name.to_string(),
                test_fn: #out_fn_iden,
                validate_fn: validate_subcommand,
                requirements: requirements,
            }
        }

        pub async fn #async_inner_fn_iden(client: Arc<Fluvio>, mut test_case: TestCase) -> Result<TestResult, TestResult> {
            use fluvio::Fluvio;
            use fluvio_test_util::test_meta::{TestCase, TestResult};
            use fluvio_test_util::test_meta::environment::{EnvDetail};
            use fluvio_test_util::test_meta::derive_attr::TestRequirements;
            use fluvio_test_util::test_meta::TestTimer;
            use fluvio_test_util::test_runner::FluvioTest;
            use fluvio_test_util::setup::environment::EnvironmentType;
            use fluvio_future::task::run;
            use fluvio_future::timer::sleep;
            use std::{io, time::Duration};
            use tokio::select;
            use std::panic::panic_any;
            use bencher::bench;

            let test_reqs : TestRequirements = serde_json::from_str(#fn_test_reqs_str).expect("Could not deserialize test reqs");
            //let test_reqs : TestRequirements = #fn_test_reqs;

            // Customize test environment if it meets minimum requirements
            if FluvioTest::is_env_acceptable(&test_reqs, &test_case) {

                // Test-level environment customizations from macro attrs
                FluvioTest::customize_test(&test_reqs, &mut test_case);

                // Create topic before starting test
                FluvioTest::create_topic(client.clone(), &test_case.environment)
                    .await
                    .expect("Unable to create default topic");

                // Wrap the user test in a closure
                // If the test is a benchmark, we want to build this in a specific way
                let test_fn = |client: Arc<Fluvio>, test_case: TestCase| async {
                    #test_body
                };

                // start a timeout timer
                let timeout_duration = test_case.environment.timeout();
                let mut timeout_timer = sleep(timeout_duration.clone());

                // Start a test timer for the user's test now that setup is done
                let mut test_timer = TestTimer::start();

                select! {
                    _ = &mut timeout_timer => {
                        test_timer.stop();
                        eprintln!("\nTest timed out ({:?})", timeout_duration);
                        //let _ = std::panic::take_hook();
                        Err(TestResult {
                            success: false,
                            duration: test_timer.duration(),
                        })
                    },

                    _ = test_fn(client, test_case) => {
                        test_timer.stop();

                        Ok(TestResult {
                            success: true,
                            duration: test_timer.duration(),
                        })
                    }
                }

            } else {
                println!("Test skipped...");

                Ok(TestResult {
                    success: true,
                    duration: Duration::new(0, 0),
                })
            }
        }

    };

    output_fn.into()
}
