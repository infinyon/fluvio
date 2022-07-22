use proc_macro::TokenStream;
use proc_macro2::Span;

use syn::Ident;
use quote::quote;

fn impl_test_case_macro(opt_struct: &syn::DeriveInput) -> TokenStream {
    let options_struct_name_ident = &opt_struct.ident;

    let test_case_struct_name_str = "MyTestCase".to_string();
    let test_case_struct_ident = Ident::new(&test_case_struct_name_str, Span::call_site());

    let gen = quote! {

        use fluvio_test_util::test_meta::{TestOption, TestCase};

        #[derive(Debug, Clone)]
        pub struct #test_case_struct_ident {
            pub environment: fluvio_test_util::test_meta::environment::EnvironmentSetup,
            pub option: #options_struct_name_ident,
        }

        impl From<fluvio_test_util::test_meta::TestCase> for #test_case_struct_ident {
            fn from(test_case: fluvio_test_util::test_meta::TestCase) -> Self {
                let user_option = test_case
                    .option
                    .as_any()
                    .downcast_ref::<#options_struct_name_ident>()
                    .expect("Error creating TestCase for {options_struct_name_str}")
                    .to_owned();
                Self {
                    environment: test_case.environment,
                    option: user_option,
                }
            }
        }

        impl fluvio_test_util::test_meta::TestOption for #options_struct_name_ident {
            fn as_any(&self) -> &dyn std::any::Any {
                self
            }
        }

    };

    gen.into()
}

#[proc_macro_derive(MyTestCase)]
pub fn fluvio_test_case_derive(input: TokenStream) -> TokenStream {
    let ast = syn::parse(input).unwrap();

    impl_test_case_macro(&ast)
}
