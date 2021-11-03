use tracing::debug;
use structopt::StructOpt;

use fluvio_controlplane_metadata::smartstream::{SmartStreamInputs, SmartStreamModules};

use fluvio::Fluvio;
use fluvio::metadata::smartstream::{
    SmartStreamSpec, SmartStreamModuleRef, SmartStreamInput, SmartStreamRef,
};

use crate::Result;

/// Create a new SmartModule with a given name
#[derive(Debug, StructOpt)]
pub struct CreateSmartStreamOpt {
    name: String,

    #[structopt(long)]
    left: String,

    #[structopt(long, required_if("left", "false"))]
    leftstream: bool,

    #[structopt(long)]
    right: Option<String>,

    #[structopt(long, required_if("right", "false"))]
    rightstream: bool,

    /// list of transforms to apply to the stream, this is order list of modules
    /// ex:  foo,bar,baz
    #[structopt(short = "t", long= "transforms",parse(try_from_str = parse_module))]
    transforms: ModuleList,
}

impl CreateSmartStreamOpt {
    pub async fn process(self, fluvio: &Fluvio) -> Result<()> {
        let (name, spec): (String, SmartStreamSpec) = self.into();
        debug!(%name,?spec,"creating smartstream");

        let admin = fluvio.admin().await;
        admin.create(name.clone(), false, spec).await?;
        println!("smartstream \"{}\" created", name);

        Ok(())
    }
}

/// convert Create Option into
impl Into<(String, SmartStreamSpec)> for CreateSmartStreamOpt {
    fn into(self) -> (String, SmartStreamSpec) {
        let left = if self.leftstream {
            SmartStreamInput::SmartStream(SmartStreamRef::new(self.left))
        } else {
            SmartStreamInput::Topic(SmartStreamRef::new(self.left))
        };

        let right_flag = self.rightstream;
        let right = self.right.map(move |r| {
            if right_flag {
                SmartStreamInput::SmartStream(SmartStreamRef::new(r))
            } else {
                SmartStreamInput::Topic(SmartStreamRef::new(r))
            }
        });

        (
            self.name,
            SmartStreamSpec {
                inputs: SmartStreamInputs { left, right },

                modules: SmartStreamModules {
                    transforms: self.transforms.modules(),
                    outputs: vec![],
                },
            },
        )
    }
}

#[derive(Debug, PartialEq)]
struct ModuleList(Vec<SmartStreamModuleRef>);

impl ModuleList {
    fn modules(self) -> Vec<SmartStreamModuleRef> {
        self.0
    }
}

/// parse stream module
fn parse_module(src: &str) -> Result<ModuleList, std::io::Error> {
    let modules: Vec<SmartStreamModuleRef> = src
        .split(',')
        .map(|s| s.trim().to_string())
        .map(|s| SmartStreamModuleRef::new(s))
        .collect();
    Ok(ModuleList(modules))
}

#[cfg(test)]
mod test {

    use super::*;

    #[test]
    fn test_module_parse() {
        assert_eq!(
            parse_module("foo,bar,baz").expect("parse"),
            ModuleList(vec![
                SmartStreamModuleRef::new("foo".to_string()),
                SmartStreamModuleRef::new("bar".to_string()),
                SmartStreamModuleRef::new("baz".to_string()),
            ])
        );
    }
}
