use std::ptr;
use std::ffi::CString;

use crate::sys::napi_property_descriptor;
use crate::sys::napi_property_attributes_napi_default;
use crate::sys::napi_callback_raw;
use crate::sys::napi_callback;

#[derive(Debug,Clone)]
pub struct Property {
    name: CString,
    method: napi_callback,
    getter: napi_callback,
    setter: napi_callback
}

impl Property {

    pub fn new(name: &str) -> Self {
        Self {
            name: CString::new(name).expect("c-string should not fail"),
            method: None,
            getter: None,
            setter: None
        }
    }

    pub fn method(mut self,method: napi_callback_raw) -> Self {

        self.method = Some(method);
        self
    }

    pub fn getter(mut self,getter: napi_callback_raw) -> Self {
        self.getter = Some(getter);
        self
    }

    pub fn as_raw_property(&self) -> napi_property_descriptor {

        napi_property_descriptor {
            utf8name: self.name.as_ptr(),
            name: ptr::null_mut(),
            method: self.method,
            getter: self.getter,
            setter: self.setter,
            value: ptr::null_mut(),
            attributes: napi_property_attributes_napi_default,
            data: ptr::null_mut()
        }
    }

}




pub struct PropertiesBuilder(Vec<Property>);

impl PropertiesBuilder {

    pub fn new() -> Self {
        Self(vec![])
    }

    pub fn add(mut self, property: Property) -> Self {
       self.0.push(property);
       self
    }

    pub fn mut_add(&mut self, property: Property) {
        self.0.push(property);
     }

    // define into env
    pub fn as_raw_properties(&self) ->  Vec<napi_property_descriptor> {

        self.0.iter().map( |p| p.as_raw_property()).collect()
        
    }
}

impl From<Vec<Property>> for PropertiesBuilder {
    fn from(properties: Vec<Property>) -> Self {
        Self(properties)
    }
}
