
use nj::derive::node_bindgen;


#[node_bindgen]
fn hello<F: Fn(String)>(first: f64, second: F) {

    let msg = format!("argument is: {}", first);

    second(msg);
}

/*
is same as below
#[no_mangle]
pub extern "C" fn hello_callback(env: napi_env, info: napi_callback_info) -> napi_value {

    fn hello<F>(first: f64, second: F) where F: Fn(String) {

        let msg = format!("argument is: {}", first);

        second(msg);
    }

    let js_env = JsEnv::new(env);

    let result: Result<napi_value,NjError> = (  || {

        
        let cb = js_env.get_cb_info(info, 2)?;
    
        let r_arg1 = cb.get_value::<f64>(0)?;
        let r_arg2 = cb.get_value::<JsCallbackFunction>(1)?;
    
        println!("got arguments");
        
        hello(r_arg1,| cb_arg1: String| {

            let result: Result<napi_value,NjError> = (|| {
                let args = vec![
                    cb_arg1.to_js(&js_env)?,
                ];
                r_arg2.call(args,&js_env)
            })();
           
            result.to_js(&js_env); 
        }).to_js(&js_env)
    })();

    result.to_js(&js_env)

   
}


use nj::core::Property;
use nj::core::submit_property;

#[nj::core::ctor]
fn test() {
    println!("init property");
    let property = Property::new("hello").method(hello_callback);
    submit_property(property);
}
*/
