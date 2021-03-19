use proc_macro::TokenStream;
use proc_macro2::Span;

use fluvio_test_util::test_meta::TestRequirements;
use syn::{AttributeArgs, Ident, ItemFn, parse_macro_input};
use quote::quote;

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
///    use fluvio_test_util::test_meta::TestRequirements;
///    let test_reqs : TestRequirements = [...]
///    if (test_min_spu #) > (test_runner_spu #) {
///        println!("Test requires {} spu", (test_min_spu #));
///        println!("Test skipped..."");
///    } else {
///        // Create topic before starting test
///        {...}
///
///        println!("Test")
///    }
/// }
#[proc_macro_attribute]
pub fn fluvio_test(args: TokenStream, input: TokenStream) -> TokenStream {
    let fn_test_reqs: TestRequirements =
        match TestRequirements::from_ast(parse_macro_input!(args as AttributeArgs)) {
            Ok(attr) => attr,
            Err(_err) => panic!("Parse failed"),
        };

    let fn_test_reqs_str =
        serde_json::to_string(&fn_test_reqs).expect("Could not serialize test reqs");

    // Read the user test
    let fn_user_test = parse_macro_input!(input as ItemFn);

    // If test name is given, then use that instead of the test's function name
    let test_name = if let Some(req_test_name) = &fn_test_reqs.test_name {
        Ident::new(&req_test_name.to_string(), Span::call_site())
    } else {
        fn_user_test.sig.ident
    };

    let test_body = &fn_user_test.block;

    let out_fn_iden = Ident::new(&test_name.to_string(), Span::call_site());

    let output_fn = quote! {

        pub async fn #out_fn_iden(client: Arc<Fluvio>, mut test_case: TestCase) {
            use fluvio::Fluvio;
            use fluvio_test_util::test_meta::{TestRequirements, TestCase, EnvDetail};
            use fluvio_test_util::test_runner::FluvioTest;
            use fluvio_future::task::run_block_on;
            use fluvio_future::timer::sleep;
            use std::{io, time::Duration};
            use tokio::select;
            use fluvio_test_util::setup::environment::EnvironmentType;

            let test_reqs : TestRequirements = serde_json::from_str(#fn_test_reqs_str).expect("Could not deserialize test reqs");
            //let test_reqs : TestRequirements = #fn_test_reqs;

            // Move this into TestRunner
            let mut precheck_pass = true;
            if let Some(min_spu) = test_reqs.min_spu {
                if min_spu > test_case.environment.spu() {
                    precheck_pass = false;
                    println!("Test requires {} spu", min_spu);
                }
            }

            // if `cluster_type` undefined, no cluster restrictions
            // if `cluster_type = local` is defined, then environment must be local or skip
            // if `cluster_type = k8`, then environment must be k8 or skip
            if let Some(cluster_type) = test_reqs.cluster_type {
                if test_case.environment.cluster_type() != cluster_type {
                    println!("Test requires cluster type {:?} ", cluster_type);
                    precheck_pass = false;
                }
            }

            if precheck_pass {
                if let Some(topic) = test_reqs.topic {
                    test_case.environment.set_topic_name(topic);
                }

                // Set timer
                if let Some(timeout) = test_reqs.timeout {
                    test_case.environment.set_timeout(timeout)
                }

                // Create topic before starting test
                FluvioTest::create_topic(client.clone(), &test_case.environment)
                    .await
                    .expect("Unable to create default topic");

                // start a timeout timer
                let mut timer = sleep(Duration::from_secs(test_case.environment.timeout().into()));

                // TODO Take a timestamp

                let test_fn = |client: Arc<Fluvio>, test_case: TestCase| async {
                    #test_body
                };

                select! {
                    _ = &mut timer => {
                        panic!("timer expired");
                    },

                    _ = test_fn(client, test_case) => {
                        println!("Test completed");
                        // TODO Take another timestamp
                        // TODO Calculate run time
                    }
                }

            } else {
                println!("Test skipped...");
            }
        }

    };

    output_fn.into()
}
