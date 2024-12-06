use proc_macro2::TokenStream;
use quote::quote;
use syn::Path;

use crate::ast::{ConnectorFn, ConnectorDirection, ConnectorConfigStruct};

pub(crate) fn generate_connector(direction: ConnectorDirection, func: &ConnectorFn) -> TokenStream {
    match direction {
        ConnectorDirection::Source => generate_source(func),
        ConnectorDirection::Sink => generate_sink(func),
    }
}

fn generate_source(func: &ConnectorFn) -> TokenStream {
    let user_fn = &func.name;
    let user_code = &func.func;

    let init_and_parse_config = init_and_parse_config(func.config_type_path);
    quote! {

        fn main() -> ::fluvio_connector_common::Result<()> {
            #init_and_parse_config
            let stop_signal = ::fluvio_connector_common::consumer::init_ctrlc()?;

            ::fluvio_connector_common::future::run_block_on(async {
                let (fluvio, producer) = ::fluvio_connector_common::producer::producer_from_config(&common_config).await?;

                let metrics = ::std::sync::Arc::new(::fluvio_connector_common::monitoring::ConnectorMetrics::new(fluvio.metrics()));
                ::fluvio_connector_common::monitoring::init_monitoring(metrics);

                ::fluvio_connector_common::future::select! {
                    user_fn_result = async {
                        #user_fn(user_config, producer).await
                    } => {
                        match user_fn_result {
                            Ok(_) => ::fluvio_connector_common::tracing::info!("Connector arrived at end of stream"),
                            Err(e) => {
                                ::fluvio_connector_common::tracing::error!(%e, "Error encountered producing records in source connector");
                                return Err(e.into());
                            },
                        }
                    },
                    _ = stop_signal.recv() => {
                        ::fluvio_connector_common::tracing::info!("Stop signal received, shutting down connector.");
                    },
                };
                Ok(()) as ::fluvio_connector_common::Result<()>
            })?;

            Ok(())
        }

        #user_code
    }
}

fn generate_sink(func: &ConnectorFn) -> TokenStream {
    let user_fn = &func.name;
    let user_code = &func.func;

    let init_and_parse_config = init_and_parse_config(func.config_type_path);
    quote! {

        fn main() -> ::fluvio_connector_common::Result<()> {
            #init_and_parse_config
            let stop_signal = ::fluvio_connector_common::consumer::init_ctrlc()?;

            ::fluvio_connector_common::future::run_block_on(async {
                let (fluvio, mut stream) = ::fluvio_connector_common::consumer::consumer_stream_from_config(&common_config).await?;

                let metrics = ::std::sync::Arc::new(::fluvio_connector_common::monitoring::ConnectorMetrics::new(fluvio.metrics()));
                ::fluvio_connector_common::monitoring::init_monitoring(metrics);

                ::fluvio_connector_common::future::select! {
                    user_fn_result = async {
                        #user_fn(user_config, stream).await
                    } => {
                        match user_fn_result {
                            Ok(_) => ::fluvio_connector_common::tracing::info!("Connector arrived at end of stream"),
                            Err(e) => {
                                ::fluvio_connector_common::tracing::error!(%e, "Error encountered processing records in sink connector");
                                return Err(e.into());
                            },
                        }
                    },
                    _ = stop_signal.recv() => {
                        ::fluvio_connector_common::tracing::info!("Stop signal received, shutting down connector.");
                    },
                };
                Ok(()) as ::fluvio_connector_common::Result<()>
            })?;

            Ok(())
        }

        #user_code
    }
}

fn init_and_parse_config(config_type_path: &Path) -> TokenStream {
    quote! {
        #[derive(Debug)]
        pub struct ConnectorOpt {
            config: ::std::path::PathBuf,
            secrets: Option<::std::path::PathBuf>
        }

        impl ConnectorOpt {
            fn parse() -> Self {
                let path = ::std::env::args()
                    .enumerate()
                    .find(|(_, a)| a.eq("--config"))
                    .and_then(|(i, _)| ::std::env::args().nth(i + 1))
                    .map(::std::path::PathBuf::from);
                let secrets = ::std::env::args()
                    .enumerate()
                    .find(|(_, a)| a.eq("--secrets"))
                    .and_then(|(i, _)| ::std::env::args().nth(i + 1))
                    .map(::std::path::PathBuf::from);

                match path {
                    Some(config) => Self {config, secrets},
                    None => {
                        eprintln!("error: The following required arguments were not provided:\n  --config <PATH>");
                        ::std::process::exit(1)
                    }
                }
            }
        }

        ::fluvio_connector_common::future::init_logger();

        let opts = ConnectorOpt::parse();

        match &opts.secrets {
            Some(secrets) => {
                ::fluvio_connector_common::tracing::info!("Using FileSecretStore");
                ::fluvio_connector_common::secret::set_default_secret_store(
                    ::std::boxed::Box::new(::fluvio_connector_common::secret::FileSecretStore::from(secrets)))?;
            },
            None => {
                ::fluvio_connector_common::tracing::info!("Using EnvSecretStore");
                ::fluvio_connector_common::secret::set_default_secret_store(
                    ::std::boxed::Box::new(::fluvio_connector_common::secret::EnvSecretStore))?;
            }
        };

        ::fluvio_connector_common::tracing::info!("Reading config file from: {}", opts.config.to_string_lossy());

        let config_str = ::std::fs::read_to_string(opts.config.as_path())?;
        ::fluvio_connector_common::tracing::debug!(%config_str, "input config");

        /// Resolve any secrets/env in the config
        let config_str_resolved =::fluvio_connector_common::render_config_str(&config_str)?;

        let config_value = ::fluvio_connector_common::config::value_from_reader(config_str_resolved.as_bytes())?;

        let common_config = ::fluvio_connector_common::config::ConnectorConfig::from_value(config_value.clone())?;

        let user_config: #config_type_path = ::fluvio_connector_common::config::from_value(config_value, Some(#config_type_path::__config_name()))?;

        ::fluvio_connector_common::tracing::info!(conn_type=common_config.r#type(), conn_name=common_config.name(), conn_version=common_config.version(), "Starting Processing");
    }
}

pub(crate) fn generate_connector_config(item: &ConnectorConfigStruct) -> TokenStream {
    let config_struct = item.item_struct;
    let ident = &item.item_struct.ident;
    let config_name = &item.config_name;

    quote! {
        #[derive(serde::Deserialize)]
        #config_struct

        impl #ident {
            pub fn __config_name() -> &'static str {
                #config_name
            }

        }
    }
}
