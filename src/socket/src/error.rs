#[cfg(not(target_arch = "wasm32"))]
use fluvio_future::zero_copy::SendFileError;

use std::io::Error as IoError;
use thiserror::Error;

#[cfg(target_arch = "wasm32")]
use wasm_bindgen::JsValue;


#[derive(Error, Debug)]
pub enum FlvSocketError {
    #[error(transparent)]
    IoError(#[from] IoError),

    #[error("Socket closed")]
    SocketClosed,

    #[cfg(not(target_arch = "wasm32"))]
    #[error("Zero-copy IO error")]
    SendFileError(#[from] SendFileError),

    #[error("JS Error: `{0:?}`")]
    #[cfg(target_arch = "wasm32")]
    JsError(String),
}

#[cfg(target_arch = "wasm32")]
impl From<JsValue> for FlvSocketError {
    fn from(e: JsValue) -> Self {
        Self::JsError(
            format!("{:?}", e)
        )
    }
}
#[cfg(target_arch = "wasm32")]
impl From<async_channel::RecvError>for FlvSocketError {
    fn from(_: async_channel::RecvError) -> Self {
        unimplemented!()
    }

}
