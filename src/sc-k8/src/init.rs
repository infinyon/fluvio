//!
//! # Iitialization routines for Streaming Coordinator (SC)
//!
//! All processing engines are hooked-up here. Channels are created and split between sencders
//! and receivers.
//!

use flv_future_aio::task::main;

use k8_client::new_shared;
use flv_sc_core::start_main_loop;
use crate::cli::parse_cli_or_exit;
use crate::operator::run_k8_operators;

const VERSION: &'static str = env!("CARGO_PKG_VERSION");

pub fn main_k8_loop() {
   
    // parse configuration (program exits on error)
    let (sc_config, k8_config) = parse_cli_or_exit();

    println!("starting sc server with k8: {}", VERSION);

    main(async move {
        // init k8 service
        let k8_client = new_shared(k8_config).expect("problem creating k8 client");
        let namespace = sc_config.namespace.clone();
        let (ws_service, metadata) = start_main_loop(sc_config, k8_client).await;
        run_k8_operators(ws_service.clone(), namespace.clone(), metadata.owned_spus());

        println!("Streaming Controller started successfully");
    });
}
