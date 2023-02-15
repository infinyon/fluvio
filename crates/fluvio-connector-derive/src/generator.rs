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

            ::fluvio_connector_common::future::run_block_on(async {
                let (fluvio, producer) = ::fluvio_connector_common::producer::producer_from_config(&common_config).await?;

                let metrics = ::std::sync::Arc::new(::fluvio_connector_common::monitoring::ConnectorMetrics::new(fluvio.metrics()));
                ::fluvio_connector_common::monitoring::init_monitoring(metrics);

                #user_fn(user_config, producer).await
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

            ::fluvio_connector_common::future::run_block_on(async {
                let (fluvio, stream) = ::fluvio_connector_common::consumer::consumer_stream_from_config(&common_config).await?;

                let metrics = ::std::sync::Arc::new(::fluvio_connector_common::monitoring::ConnectorMetrics::new(fluvio.metrics()));
                ::fluvio_connector_common::monitoring::init_monitoring(metrics);

                #user_fn(user_config, stream).await
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

        let config_value = ::fluvio_connector_common::config::value_from_file(opts.config.as_path())?;
        ::fluvio_connector_common::tracing::trace!("{:#?}", config_value);

        let common_config = ::fluvio_connector_common::config::ConnectorConfig::from_value(config_value.clone())?;
        ::fluvio_connector_common::tracing::debug!("{:#?}", common_config);

        let user_config: #config_type_path = ::fluvio_connector_common::config::from_value(config_value, Some(#config_type_path::__config_name()))?;

        ::fluvio_connector_common::tracing::info!("starting processing");
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
