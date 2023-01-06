use syn::{
    AttributeArgs, Result, Error, NestedMeta, Meta, spanned::Spanned, ItemFn, Ident, FnArg, Path,
    Type, Lit, ItemStruct,
};

pub(crate) enum ConnectorDirection {
    Source,
    Sink,
}

impl ConnectorDirection {
    pub(crate) fn from_ast(args: &AttributeArgs) -> Result<Self> {
        args.iter()
            .find_map(|item| match item {
                NestedMeta::Meta(Meta::Path(p)) => p.segments.iter().last().and_then(|p_it| {
                    match p_it.ident.to_string().as_str() {
                        "source" => Some(Self::Source),
                        "sink" => Some(Self::Sink),
                        _ => None,
                    }
                }),
                _ => None,
            })
            .ok_or_else(|| Error::new(args[0].span(), "Missing Connector type. Supported: '#[connector(source)]' and '#[connector(sink)]'"))
    }
}

pub struct ConnectorFn<'a> {
    pub name: &'a Ident,
    pub func: &'a ItemFn,
    pub config_type_path: &'a Path,
}

impl<'a> ConnectorFn<'a> {
    pub fn from_ast(func: &'a ItemFn) -> Result<Self> {
        func.sig
            .asyncness
            .as_ref()
            .ok_or_else(|| Error::new(func.span(), "Connector function must be async"))?;
        if func.sig.inputs.len() != 2 {
            return Err(Error::new(
                func.span(),
                "Connector function must have two input arguments",
            ));
        };
        let config_type_path = config_type_path(&func.sig.inputs[0])?;
        let name = &func.sig.ident;
        Ok(Self {
            name,
            func,
            config_type_path,
        })
    }
}

pub struct ConnectorConfigStruct<'a> {
    pub item_struct: &'a ItemStruct,
    pub config_name: String,
}

impl<'a> ConnectorConfigStruct<'a> {
    pub fn from_ast(args: &AttributeArgs, item_struct: &'a ItemStruct) -> Result<Self> {
        args.iter()
            .find(|item| match item {
                NestedMeta::Meta(Meta::Path(p)) => p.is_ident("config"),
                _ => false,
            })
            .ok_or_else(|| {
                Error::new(
                    item_struct.span(),
                    "struct must be annotated as config, e.g. '#[connector(config)]'",
                )
            })?;
        let config_name = config_name(args)?;
        if config_name.eq("transforms") | config_name.eq("meta") {
            return Err(Error::new(
                item_struct.span(),
                "Custom config name conflicts with reserved names: 'meta' and 'transforms'",
            ));
        }
        Ok(Self {
            item_struct,
            config_name,
        })
    }
}

fn config_type_path(arg: &FnArg) -> Result<&Path> {
    match arg {
        FnArg::Receiver(_) => Err(Error::new(
            arg.span(),
            "config input argument must not be self",
        )),
        FnArg::Typed(pat_type) => match pat_type.ty.as_ref() {
            Type::Path(type_path) => Ok(&type_path.path),
            _ => Err(Error::new(
                arg.span(),
                "config type must valid path of owned type",
            )),
        },
    }
}

fn config_name(args: &AttributeArgs) -> Result<String> {
    for arg in args {
        match arg {
            NestedMeta::Meta(Meta::NameValue(name_value)) if name_value.path.is_ident("name") => {
                if let Lit::Str(lit_str) = &name_value.lit {
                    return Ok(lit_str.value());
                }
            }
            _ => {}
        }
    }
    Ok("custom".to_owned())
}
