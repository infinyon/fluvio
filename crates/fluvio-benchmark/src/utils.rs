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

pub fn nanos_to_ms_pritable(nano: u64) -> String {
    pretty_duration(Duration::from_nanos(nano))
}

pub fn pretty_duration(d: Duration) -> String {
    let nanos = d.as_nanos();
    // 1 ns = 1
    // 1 µs = 1,000 ns
    // 1 ms = 1,000,000 ns
    // 1 s  = 1,000,000,000 ns
    // 1 m  = 60 s

    if nanos < 1_000 {
        // Less than 1µs, display in ns
        format!("{nanos}ns")
    } else if nanos < 1_000_000 {
        // Less than 1ms, display in µs
        let us = nanos as f64 / 1_000.0;
        format!("{us:.1}µs")
    } else if nanos < 1_000_000_000 {
        // Less than 1s, display in ms
        let ms = nanos as f64 / 1_000_000.0;
        format!("{ms:.1}ms")
    } else {
        // Now we’re at least 1 second
        let secs = nanos as f64 / 1_000_000_000.0;
        if secs < 60.0 {
            // Less than a minute, display in seconds
            format!("{secs:.1}s")
        } else {
            // Otherwise, display in minutes
            let mins = secs / 60.0;
            format!("{mins:.1}m")
        }
    }
}

/// Calculate the number of records each producer should send
pub fn records_per_producer(id: u64, num_producers: u64, num_records: u64) -> u64 {
    if id == 0 {
        num_records / num_producers + num_records % num_producers
    } else {
        num_records / num_producers
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_get_num_records_per_producer() {
        let num_producers = 3;
        let num_records = 10;

        assert_eq!(records_per_producer(0, num_producers, num_records), 4);
        assert_eq!(records_per_producer(1, num_producers, num_records), 3);
        assert_eq!(records_per_producer(2, num_producers, num_records), 3);

        let num_producers = 3;
        let num_records = 12;
        assert_eq!(records_per_producer(0, num_producers, num_records), 4);
        assert_eq!(records_per_producer(1, num_producers, num_records), 4);
        assert_eq!(records_per_producer(2, num_producers, num_records), 4);
    }
}
