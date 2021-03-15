use proc_macro::TokenStream;

#[proc_macro]
pub fn validate_subcommand(_: TokenStream) -> TokenStream {
    let fn_str = "
fn test_names() -> Vec<String> {
    let mut test_names = Vec::new();
    for test in inventory::iter::<FluvioTest> {
        test_names.push(test.name.clone());
    }
    test_names
}

fn validate_subcommand(subcommand: Vec<String>) -> Result<Box<dyn TestOption>, ()> {
    let test_name = subcommand[0].clone();
    let test_names = test_names();

    if test_names.contains(&test_name) {
        match test_name.as_str() {
            \"smoke\" => Ok(Box::new(fluvio_integration_tests::smoke::SmokeTestOption::from_iter(subcommand))),
            \"concurrent\" => Ok(Box::new(fluvio_integration_tests::concurrent::ConcurrentTestOption::from_iter(subcommand))),
            _ => unreachable!(\"This shouldn't be reachable\"),
        }
    } else {
        Err(())
    }
}";

    fn_str.parse().unwrap()
}
