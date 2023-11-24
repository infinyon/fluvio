use std::str::FromStr;

use proc_macro2::Span;
use syn::{
    meta::ParseNestedMeta, punctuated::Punctuated, spanned::Spanned, Attribute, Expr, ExprLit,
    ExprPath, ExprUnary, Lit, LitStr, Meta, Token, UnOp,
};

use crate::ast::prop::PropAttrsType;

/// Parses the specified attributes from a `syn::Attribute` iterator.
/// # Arguments
/// * `attrs` - `&[Attribute]` iterator
/// * `attr_ident` - The path ident to search for
/// * `meta` - a return variable that will give us the parse_nested_meta of each element
/// * `field` - nested path to search for
/// * `opt` - check if attribute is already specified
/// * `block` - block of code to execute
macro_rules! parse_attributes {
    ($attrs:expr, $attr_ident:expr, $meta: ident, $($field:pat, $opt:expr => $block:expr)*) => {
        for attr in $attrs {
            if !attr.path().is_ident($attr_ident) {
                continue;
            }

            attr.parse_nested_meta(|$meta| {

                let ident = $meta.path.get_ident().ok_or_else(|| $meta.error(concat!("unrecognized ", $attr_ident, " attribute")))?;
                let attr_name = &ident.to_string();

                match attr_name.as_str() {
                    $(
                        $field if $opt.is_none() => {
                            return Ok($block)
                        },
                        $field => {return Err($meta.error(concat!($attr_ident, " attribute already specified")))}
                    )*

                    _ => return Err($meta.error(concat!("unrecognized ", $attr_ident, " attribute"))),
                }
            })?;
        }
    };
}

// This piece of code can be included in the macro but there are some issues
// For example if we return this from the parse_attributes! macro with closure function
// It works but there is an issue with clippy giving a "warning: try not to call a closure in the expression where it is declared"
// Here is how this can be fixed if used in macro: https://stackoverflow.com/questions/76989679/how-to-call-a-closure-from-a-macro
// So to do this in a less work around way we use a separate function like so:
// We return $meta variable like shown here: https://github.com/rust-lang/rust-clippy/issues/1553
pub fn parse_attributes_data(
    meta: &ParseNestedMeta,
) -> syn::Result<(Option<syn::Expr>, Span, String)> {
    // we can safely unwarp as this is already checked from the parse_attributes macro
    let ident = match meta.path.get_ident() {
        Some(value) => Ok(value),
        None => Err(syn::Error::new(
            meta.input.span(),
            "Path ident not found".to_string(),
        )),
    }?;

    // to_string so we can return the name of the ident
    // helps to reduce the repetitiveness of attribute names
    let attr_name = ident.to_string();
    let attr_span = ident.span();

    if let Ok(value) = meta.value() {
        if let Ok(expr) = value.parse() {
            return Ok((Some(expr), attr_span, attr_name));
        }
    }

    Ok((None, attr_span, attr_name))
}

pub(crate) use parse_attributes;

pub fn get_attr_type_from_meta(meta: &ParseNestedMeta) -> syn::Result<PropAttrsType> {
    let (expr, attr_span, attr_name) = parse_attributes_data(meta)?;
    get_expr_value(&attr_name, expr.as_ref(), attr_span)
}
pub fn get_expr_value<'a>(
    attr_name: &'a str,
    field: Option<&'a Expr>,
    span: Span,
) -> syn::Result<PropAttrsType> {
    match &field {
        Some(Expr::Lit(lit_expr)) => {
            if let Lit::Int(lit) = &lit_expr.lit {
                Ok(PropAttrsType::Int(lit.base10_parse::<i16>()?))
            } else if let Lit::Str(lit) = &lit_expr.lit {
                let value = &lit.value();

                // To handle functions passed as lit we check for ()
                // And strip it when creating the new Ident so we can use it later on
                if value.contains('(') && value.contains(')') {
                    if let Some(value) = &lit.value().strip_suffix("()") {
                        dbg!(value);
                        return Ok(PropAttrsType::Fn(syn::Ident::new(value.trim(), lit.span())));
                    }
                }

                Ok(PropAttrsType::Lit(syn::Ident::new(value, lit.span())))
            } else {
                Err(syn::Error::new(
                    span,
                    format!("Expected {attr_name} attribute to be a LitStr or LitInt: `{attr_name} = \"...\"`"),
                ))
            }
        }
        Some(Expr::Unary(ExprUnary { expr, .. })) => {
            // When passing -1 as a value it is returned as type Unary
            // So to handle that we are checking if it's Unary Lit and continue as usual
            // If needed this can be extended to handle the Unary operators
            // But it doesn't seem that is necessary currently
            Ok(PropAttrsType::Int(parse_int_expr(attr_name, expr, span)?))
        }
        Some(Expr::Path(ExprPath { path, .. })) => {
            // For now we only need Path just to handle cases where CONSTANTS are being passed without "" quotes
            if let Some(path_ident) = path.get_ident() {
                return Ok(PropAttrsType::Lit(syn::Ident::new(
                    &path_ident.to_string(),
                    path_ident.span(),
                )));
            }

            Err(syn::Error::new(
                span,
                format!("Attribute {attr_name} needs to be a valid Path: `{attr_name} = \"...\"`"),
            ))
        }
        _ => Err(syn::Error::new(
            span,
            format!(
                "Expected {attr_name} attribute to be a Lit/Path/Unary: `{attr_name} = \"...\"`"
            ),
        )),
    }
}

fn parse_int_expr<'a, T>(attr_name: &'a str, expr: &'a Expr, span: Span) -> syn::Result<T>
where
    T: std::ops::Neg<Output = T> + FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    match expr {
        Expr::Unary(ExprUnary {
            op: UnOp::Neg(_),
            expr,
            ..
        }) => parse_int_expr::<T>(attr_name, expr, span).map(|int| -int),
        Expr::Lit(ExprLit {
            lit: Lit::Int(int), ..
        }) => int.base10_parse(),
        _ => Err(syn::Error::new(
            span,
            format!("Expected {attr_name} to be Int: `{attr_name} = \"...\"`"),
        )),
    }
}

pub fn get_expr_int_value<'a, T>(
    attr_name: &'a str,
    value: Option<&'a Expr>,
    span: Span,
) -> syn::Result<T>
where
    T: std::ops::Neg<Output = T> + FromStr,
    <T as FromStr>::Err: std::fmt::Display,
{
    if let Some(value_expr) = value {
        parse_int_expr::<T>(attr_name, value_expr, span)
    } else {
        Err(syn::Error::new(
            span,
            format!("Expected {attr_name} to be valid Expr: `{attr_name} = \"...\"`"),
        ))
    }
}
pub fn get_lit_int<'a>(
    attr_name: &'a str,
    value: Option<&'a Expr>,
    span: Span,
) -> syn::Result<&'a syn::LitInt> {
    match &value {
        Some(Expr::Lit(lit_expr)) => {
            if let Lit::Int(lit) = &lit_expr.lit {
                Ok(lit)
            } else {
                Err(syn::Error::new(
                    span,
                    format!("Expected {attr_name} attribute to be an int: `{attr_name} = \"...\"`"),
                ))
            }
        }
        _ => Err(syn::Error::new(
            span,
            format!("Expected {attr_name} attribute to be a Lit: `{attr_name} = \"...\"`"),
        )),
    }
}
pub fn get_lit_str<'a>(
    attr_name: &'a str,
    value: Option<&'a Expr>,
    span: Span,
) -> syn::Result<&'a syn::LitStr> {
    match &value {
        Some(Expr::Lit(lit_expr)) => {
            if let Lit::Str(lit) = &lit_expr.lit {
                Ok(lit)
            } else {
                Err(syn::Error::new(
                    span,
                    format!(
                        "Expected {attr_name} attribute to be a string: `{attr_name} = \"...\"`"
                    ),
                ))
            }
        }
        _ => Err(syn::Error::new(
            span,
            format!("Expected {attr_name} attribute to be a Lit: `{attr_name} = \"...\"`"),
        )),
    }
}

pub(crate) fn find_attr(attrs: &[Attribute], name: &str) -> Option<Meta> {
    attrs.iter().find_map(|a| {
        if a.meta.path().is_ident(name) {
            Some(a.meta.clone())
        } else {
            None
        }
    })
}
pub(crate) fn find_expr_from_meta(meta: &Meta, attr_name: &str) -> syn::Result<Expr> {
    if let Meta::List(ref list) = &meta {
        for item in list.parse_args_with(Punctuated::<Meta, Token![,]>::parse_terminated)? {
            if let Meta::NameValue(kv) = &item {
                if kv.path.is_ident(attr_name) {
                    return Ok(kv.value.clone());
                }
            }
        }
    }

    Err(syn::Error::new(
        meta.span(),
        format!("{attr_name} not found!"),
    ))
}
/// find name value with str value
pub(crate) fn find_string_from_meta<'a>(meta: &'a Meta, attr_name: &'a str) -> syn::Result<LitStr> {
    match find_expr_from_meta(meta, attr_name) {
        Ok(ref value) => {
            let lit_str = get_lit_str(attr_name, Some(value), meta.span())?;
            Ok(lit_str.clone())
        }
        Err(err) => Err(err),
    }
}

pub(crate) fn find_int_from_meta(meta: &Meta, attr_name: &str) -> syn::Result<u64> {
    match find_expr_from_meta(meta, attr_name) {
        Ok(ref value) => {
            let value = get_lit_int(attr_name, Some(value), meta.span())?;

            value.base10_parse::<u64>()
        }
        Err(err) => Err(err),
    }
}
