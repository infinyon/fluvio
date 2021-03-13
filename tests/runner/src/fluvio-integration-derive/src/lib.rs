use proc_macro::TokenStream;
use proc_macro2::Span;

use fluvio_test_util::test_meta::TestRequirements;
use syn::{AttributeArgs, Ident, ItemFn, parse_macro_input};
use quote::quote;

/// This macro will allow a test writer to override
/// minimum Fluvio cluster requirements for a test
///
/// Supported keys: `min_spu` (default 1)
///
/// #[fluvio_test(min_spu = 2)]
/// pub async fn run(_client: Fluvio, option: TestOption) {
///     println!("Test")
/// }
///
/// Will expand into the following pseudocode
///
/// pub async fn run(client: Fluvio, option: TestOption) {
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

    let test_name = &fn_user_test.sig.ident;
    let test_body = &fn_user_test.block;

    let out_fn_iden = Ident::new(&test_name.to_string(), Span::call_site());

    let output_fn = quote! {

        pub async fn #out_fn_iden(client: Arc<Fluvio>, option: TestOption) {
            use fluvio::Fluvio;
            use fluvio_test_util::test_meta::{TestRequirements, TestOption};
            use fluvio_test_util::test_runner;

            let test_reqs : TestRequirements = serde_json::from_str(#fn_test_reqs_str).expect("Could not deserialize test reqs");
            //let test_reqs : TestRequirements = #fn_test_reqs;

            if let Some(min_spu) = test_reqs.min_spu {
                if min_spu > option.spu {
                    println!("Test requires {} spu", min_spu);
                    println!("Test skipped...");
                }
            } else {
                let mut option = option.clone();
                if let Some(topic) = test_reqs.topic {
                    option.topic_name = topic;
                }

            // Don't create topic if we did not start a new cluster
            if option.skip_cluster_start {
                println!("Skipping topic create");
            } else {
                // Create topic before starting test
                test_runner::create_topic(&option)
                    .await
                    .expect("Unable to create default topic");
            }

                #test_body
            }
        }

    };

    output_fn.into()
}
