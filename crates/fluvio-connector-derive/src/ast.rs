use syn::{AttributeArgs, Result, Error, NestedMeta, Meta, spanned::Spanned, ItemFn, Ident};

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
        let name = &func.sig.ident;
        Ok(Self { name, func })
    }
}
