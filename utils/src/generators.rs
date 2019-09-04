//!
//! # Utils
//!
//! Utility file to generate random entities
//!

use rand::prelude::*;
use std::i32;

/// Generate a random correlation_id (0 to 65535)
pub fn rand_correlation_id() -> i32 {
    thread_rng().gen_range(0, 65535)
}

/// Generate a random client group key (50001 to 65535)
pub fn generate_group_id() -> String {
    format!("fluvio-consumer-{}", thread_rng().gen_range(50001, 65535))
}

#[allow(dead_code)]
/// Generates a random authorization secret
pub fn generate_secret() -> String {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz\
                            0123456789";

    const SECRET_SIZE: usize = 16;
    let secret: String = (0..SECRET_SIZE)
        .map(|_| {
            let idx = thread_rng().gen_range(0, CHARSET.len());
            // This is safe because `idx` is in range of `CHARSET`
            char::from(unsafe { *CHARSET.get_unchecked(idx) })
        })
        .collect();

    secret
}

#[allow(dead_code)]
/// Generates a random authorization token
pub fn generate_auth_token() -> (String, String) {
    const CHARSET: &[u8] = b"abcdefghijklmnopqrstuvwxyz\
                            0123456789";

    const ID_SIZE: usize = 6;
    let token_name: String = (0..ID_SIZE)
        .map(|_| {
            let idx = thread_rng().gen_range(0, CHARSET.len());
            // This is safe because `idx` is in range of `CHARSET`
            char::from(unsafe { *CHARSET.get_unchecked(idx) })
        })
        .collect();

    const SECRET_SIZE: usize = 16;
    let token_secret: String = (0..SECRET_SIZE)
        .map(|_| {
            let idx = thread_rng().gen_range(0, CHARSET.len());
            // This is safe because `idx` is in range of `CHARSET`
            char::from(unsafe { *CHARSET.get_unchecked(idx) })
        })
        .collect();

    (token_name, token_secret)
}

#[allow(dead_code)]
/// Generate a random session id
pub fn rand_session_id() -> i32 {
    thread_rng().gen_range(1024, i32::MAX)
}

#[allow(dead_code)]
/// Generates a random key
pub fn generate_random_key() -> String {
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                            abcdefghijklmnopqrstuvwxyz\
                            0123456789)(*&^%$#@!~";
    const SIZE: usize = 32;
    let key: String = (0..SIZE)
        .map(|_| {
            let idx = thread_rng().gen_range(0, CHARSET.len());
            // This is safe because `idx` is in range of `CHARSET`
            char::from(unsafe { *CHARSET.get_unchecked(idx) })
        })
        .collect();

    key
}
