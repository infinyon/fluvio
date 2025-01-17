use std::time::Duration;

use rand::{distributions::Alphanumeric, Rng};
use rand::{RngCore, SeedableRng};
use rand_xoshiro::Xoshiro256PlusPlus;
use rayon::prelude::*;

/// Generate a random string of a given length
pub fn generate_random_string(size: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(size)
        .map(char::from)
        .collect()
}

/// Generate random string concurrently very fast
pub fn generate_random_string_vec(num: usize, size: usize) -> Vec<String> {
    // Define the character set: 0-9, A-Z, a-z
    const CHARSET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZ\
                             abcdefghijklmnopqrstuvwxyz\
                             0123456789";
    const CHARSET_LEN: usize = CHARSET.len();

    // Use parallel iterator for generating strings concurrently
    let random_strings: Vec<String> = (0..num)
        .into_par_iter()
        .map_init(
            || Xoshiro256PlusPlus::seed_from_u64(rand::thread_rng().next_u64()),
            |rng, _| {
                // Allocate a buffer for the string
                let mut buf = vec![0u8; size];

                // Fill the buffer with random characters
                for byte in buf.iter_mut() {
                    // Generate a random index into CHARSET
                    let idx = (rng.next_u32() as usize) % CHARSET_LEN;
                    *byte = CHARSET[idx];
                }

                // Convert buffer to String safely
                unsafe { String::from_utf8_unchecked(buf) }
            },
        )
        .collect();

    random_strings
}

pub fn nanos_to_ms_pritable(nano: u64) -> f64 {
    Duration::from_nanos(nano).as_secs_f64() * 1000.0
}
