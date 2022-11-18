use std::{str::FromStr, env};

// TODO better way of doing this
pub const FLUVIO_BENCH_RECORDS_PER_BATCH: (&str, usize) = ("FLUVIO_BENCH_RECORDS_PER_BATCH", 100);
pub const FLUVIO_BENCH_RECORD_NUM_BYTES: (&str, usize) = ("FLUVIO_BENCH_RECORD_NUM_BYTES", 10);
pub const FLUVIO_BENCH_SAMPLE_SIZE: (&str, usize) = ("FLUVIO_BENCH_SAMPLE_SIZE", 10);
pub const FLUVIO_BENCH_MAX_BYTES_PER_BATCH: (&str, usize) =
    ("FLUVIO_BENCH_MAX_BYTES_PER_BATCH", 120);

pub trait EnvOrDefault<T> {
    fn env_or_default(self) -> T
    where
        T: FromStr + std::fmt::Debug,
        <T as FromStr>::Err: std::fmt::Debug;
}

impl<T> EnvOrDefault<T> for (&'static str, T)
where
    T: FromStr + std::fmt::Debug,
    <T as FromStr>::Err: std::fmt::Debug,
{
    fn env_or_default(self) -> T
    where
        T: FromStr + std::fmt::Debug,
        <T as FromStr>::Err: std::fmt::Debug,
    {
        env::var(self.0)
            .map(|s| s.parse::<T>().unwrap())
            .unwrap_or(self.1)
    }
}
