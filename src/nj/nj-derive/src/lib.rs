extern crate proc_macro;

mod function;
mod class;
mod util;
mod parser;

use function::generate_function;
use function::FunctionAttribute;
use class::generate_class;
use util::MyTypePath;
use util::MyReferenceType;
use util::rust_arg_var;
use parser::NodeItem;
use function::FunctionArgMetadata;
use function::FunctionContext;

use proc_macro::TokenStream;
use syn::AttributeArgs;


/// This turns regular rust function into N-API compatible native module
/// 
/// For example; given rust following here
/// 
///      fn sum(first: i32, second: i32) -> i32 {
///           return first+second
///      }
/// 
/// into N-API module
///     #[no_mangle]
///     pub extern "C" fn n_sum(env: napi_env, cb_info: napi_callback_info) -> napi_value {
///         fn sum(first: i32, second: i32) -> i32 {
///           return first+second
///         }
///         let js_env = JsEnv::new(env);
///         let js_cb = result_to_napi!(js_env.get_cb_info(cb_info, 2),&js_env);
///         let first = result_to_napi!(js_cb.get_value::<i32>(0),&js_env);
///         let second = result_to_napi!(js_cb.get_value::<i32>(0),&js_env);
///         sum(msg).to_js(&js_env)
///     }
#[proc_macro_attribute]
pub fn node_bindgen(args: TokenStream, item: TokenStream) -> TokenStream {

    let parsed_item = syn::parse_macro_input!(item as NodeItem);
    let attribute_args = syn::parse_macro_input!(args as AttributeArgs);
    let expand_expression = match parsed_item {
        NodeItem::Function(fn_item) => generate_function(fn_item,attribute_args),
        NodeItem::Impl(impl_item) => generate_class(impl_item)
    };
    expand_expression.into()
}

