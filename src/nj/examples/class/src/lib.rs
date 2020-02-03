
use std::time::Duration;
use std::io::Error as IoError;

use flv_future_core::sleep;

use nj::sys::napi_value;
use nj::core::val::JsCallback;
use nj::core::JSClass;
use nj::core::NjError;
use nj::core::val::JsEnv;
use nj::core::TryIntoJs;
use nj::derive::node_bindgen;

#[node_bindgen]
async fn create(val: f64) -> Result<MyObjectWrapper,IoError> {
    Ok(MyObjectWrapper{ val })
}

struct MyObjectWrapper {
    val: f64
}

impl TryIntoJs for MyObjectWrapper {

    fn try_to_js(self, js_env: &JsEnv) -> Result<napi_value,NjError> {
        let instance = TestObject::new_instance(js_env,vec![])?;
        let test_object = TestObject::unwrap(js_env,instance)?;
        test_object.set_value(self.val);
        Ok(instance)   
    }
}

struct TestObject {
    val: Option<f64>
}

#[node_bindgen]
impl TestObject {

    #[node_bindgen(constructor)]
    fn new() -> Self {
        Self { val: None }
    }

    fn set_value(&mut self,val: f64) {
        self.val.replace(val);
    }

    #[node_bindgen]
    fn value2(&self) -> f64 {
        self.val.unwrap()
    }

    // bug: need to remove duplication
    //[node_bindgen(getter)]
    fn value(&self) -> f64 {
        self.val.unwrap()
    }
}

struct MyObject {
    val: f64,
}


#[node_bindgen]
impl MyObject {

    #[node_bindgen(constructor)]
    fn new(val: f64) -> Self {
        Self { val }
    }


    #[node_bindgen]
    fn plus_one(&self) -> f64 {
        self.val + 1.0
    }

    #[node_bindgen(getter)]
    fn value(&self) -> f64 {
        self.val
    }

    /// example where we receive callback cb explicitly. 
    /// in this case, we can manually create new instance.
    /// callback must be 2nd argument and be of type JsCallback.
    #[node_bindgen]
    fn multiply(&self, cb: &JsCallback, arg: f64) -> Result<napi_value, NjError> {
        
        let new_val = cb.env().create_double(arg * self.val)?;
        Self::new_instance(cb.env(), vec![new_val])
    } 

    
    /// promise which result in primitive type
    #[node_bindgen]
    async fn plus_two(&self, arg: f64) -> f64 {

        println!("sleeping");
        sleep(Duration::from_secs(1)).await;
        println!("woke and adding {}",arg);
        
        self.val + arg
    }
    

    /// promise where result is arbitrary struct.
    /// returning struct must implement TryIntoJs
    /// which can create new JS instance
    #[node_bindgen]
    async fn multiply2(&self,arg: f64) -> MyObjectConstructor {

        println!("sleeping");
        sleep(Duration::from_secs(1)).await;
        println!("woke and adding {}",arg);
        
        MyObjectConstructor::new(self.val * arg)
    }



    /// loop and emit event
    #[node_bindgen]
    async fn sleep<F: Fn(String)>(&self,cb: F)  {

        println!("sleeping");
        sleep(Duration::from_secs(1)).await;
        let msg = format!("hello world");
        cb(msg);        
        
    }


}
