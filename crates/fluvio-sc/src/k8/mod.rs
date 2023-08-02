//!
//! # Initialization routines for Streaming Coordinator (SC)
//!
//! All processing engines are hooked-up here. Channels are created and split between sencders
//! and receivers.
//!

pub(crate) mod controllers;
mod objects;
pub(crate) mod migration;

#[cfg(test)]
mod fixture;
