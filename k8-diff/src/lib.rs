#![feature(nll)]

mod json;

pub trait Changes {
    type Replace;
    type Patch;

    fn diff(&self, new: &Self) -> Result<Diff<Self::Replace, Self::Patch>, DiffError>;
}

#[derive(Debug)]
pub enum DiffError {
    DiffValue, // json values are different
}

// use Option as inspiration
#[derive(Debug)]
pub enum Diff<R, P> {
    None,
    Delete,
    Patch(P),   // for non primitive type
    Replace(R), // can be used for map and list (with our without tag), works on ordered list
    Merge(R),   // need tag, works on unorderd list
}

impl<R, P> Diff<R, P> {
    pub fn is_none(&self) -> bool {
        match self {
            Diff::None => true,
            _ => false,
        }
    }

    pub fn is_delete(&self) -> bool {
        match self {
            Diff::Delete => true,
            _ => false,
        }
    }

    pub fn is_replace(&self) -> bool {
        match self {
            Diff::Replace(_) => true,
            _ => false,
        }
    }

    pub fn is_patch(&self) -> bool {
        match self {
            Diff::Patch(_) => true,
            _ => false,
        }
    }

    pub fn is_merge(&self) -> bool {
        match self {
            Diff::Replace(_) => true,
            _ => false,
        }
    }

    pub fn as_replace_ref(&self) -> &R {
        match self {
            Diff::Replace(ref val) => val,
            _ => panic!("no change value"),
        }
    }

    pub fn as_patch_ref(&self) -> &P {
        match self {
            Diff::Patch(ref val) => val,
            _ => panic!("no change value"),
        }
    }
}
