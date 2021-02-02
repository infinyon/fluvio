## Build Your Own Custom Robot Assistant

This example remakes this project described in this blog https://www.fluvio.io/blog/2021/01/bot-assistant/ in Rust. It depends on the fluvio [Rust API](https://docs.rs/fluvio/0.4.0/fluvio/) instread of fluvio [Node.js API](https://www.npmjs.com/package/@fluvio/client).

* The html static assets is in the html folder.
* The tide websocket server is server folder.
* The wasm client is in wasm folder.
* The wasm pkg will be built in pkg folder.
* Makefile.toml helps build the project.
