mod table;
mod serde;
mod describe;

use comfy_table::Table;
pub use output::Terminal;
pub use output::OutputType;

pub use table::TableOutputHandler;
use table::TableRenderer;

use self::serde::SerdeRenderer;
pub use self::serde::SerializeType;

use self::describe::DescribeObjectRender;
pub use self::describe::DescribeObjectHandler;

pub use context::RenderContext;

pub trait KeyValOutputHandler {
    fn key_values(&self) -> Vec<(String, Option<String>)>;
}

mod context {

    use std::sync::Arc;

    use async_trait::async_trait;
    use super::Terminal;

    #[async_trait]
    pub trait RenderContext {
        async fn render_on<O: Terminal>(&self, out: Arc<O>);
    }
}

pub use self::error::OutputError;

mod error {

    use serde_json::Error as SerdeJsonError;
    use serde_yaml::Error as SerdeYamlError;
    use toml::ser::Error as SerdeTomlError;

    #[derive(thiserror::Error, Debug)]
    pub enum OutputError {
        #[error(transparent)]
        SerdeJson {
            #[from]
            source: SerdeJsonError,
        },
        #[error("Fluvio client error")]
        SerdeYamlError {
            #[from]
            source: SerdeYamlError,
        },
        #[error("Fluvio client error")]
        SerdeTomlError {
            #[from]
            source: SerdeTomlError,
        },
    }
}

pub trait DisplayTable {
    fn print_std(&self, indent: u8);
}

impl DisplayTable for Table {
    fn print_std(&self, indent: u8) {
        for line in self.to_string().split('\n') {
            println!("{}{}", " ".repeat(indent as usize), line);
        }
    }
}

#[allow(clippy::module_inception)]
mod output {

    use std::sync::Arc;

    use clap::ValueEnum;
    use serde::Serialize;

    use comfy_table::Table;
    use comfy_table::{Row, Cell};

    use super::{TableOutputHandler, DisplayTable};
    use super::TableRenderer;
    use super::SerdeRenderer;
    use super::DescribeObjectHandler;
    use super::DescribeObjectRender;
    use super::KeyValOutputHandler;
    use super::SerializeType;
    use super::OutputError;

    #[derive(ValueEnum, Debug, Clone, Eq, PartialEq)]
    #[allow(non_camel_case_types)]
    pub enum OutputType {
        table,
        yaml,
        json,
        toml,
    }

    /// OutputType defaults to table formatting
    impl ::std::default::Default for OutputType {
        fn default() -> Self {
            OutputType::table
        }
    }

    /// OutputType check if table
    impl OutputType {
        pub fn is_table(&self) -> bool {
            *self == OutputType::table
        }
    }

    pub trait Terminal: Sized {
        fn print(&self, msg: &str);
        fn println(&self, msg: &str);

        fn render_list<T>(self: Arc<Self>, list: &T, mode: OutputType) -> Result<(), OutputError>
        where
            T: TableOutputHandler + Serialize,
        {
            if mode.is_table() {
                let render = TableRenderer::new(self);
                render.render(list, false);
            } else {
                let render = SerdeRenderer::new(self);
                render.render(&list, mode.into())?;
            }

            Ok(())
        }

        fn render_table<T: TableOutputHandler>(self: Arc<Self>, val: &T, indent: bool) {
            let render = TableRenderer::new(self);
            render.render(val, indent);
        }

        fn render_serde<T: Serialize>(
            self: Arc<Self>,
            val: &T,
            mode: SerializeType,
        ) -> Result<(), OutputError> {
            let render = SerdeRenderer::new(self);
            render.render(val, mode)
        }

        /// describe objects
        fn describe_objects<D>(
            self: Arc<Self>,
            objects: &[D],
            mode: OutputType,
        ) -> Result<(), OutputError>
        where
            D: DescribeObjectHandler + TableOutputHandler + KeyValOutputHandler + Serialize + Clone,
        {
            let render = DescribeObjectRender::new(self);
            render.render(objects, mode)
        }

        /// print something that can be rendered as key values
        fn render_key_values<K: KeyValOutputHandler>(&self, key_val: &K) {
            let kv_values = key_val.key_values();

            // Create the table
            let mut table = Table::new();

            for (key, val_opt) in kv_values {
                let mut row = Row::new();

                if let Some(val) = val_opt {
                    row.add_cell(Cell::new(format!("{}{}{}", key, ":".to_owned(), val)));
                } else {
                    row.add_cell(Cell::new(key.to_string()));
                }

                table.add_row(row);
            }

            table.load_preset(comfy_table::presets::NOTHING);

            // print table to stdout
            table.print_std(2);
        }
    }
}
