use std::collections::HashMap;
use fluvio_smartstream::{smartstream, Result, Record, RecordData};
use serde::{Serialize, Deserialize};

#[derive(Default, Serialize, Deserialize)]
struct GithubStars(HashMap<String, u32>);

impl std::ops::Add for GithubStars {
    type Output = Self;

    fn add(mut self, next: Self) -> Self::Output {
        for (key, new_stars) in next.0 {
            self.0
                .entry(key)
                .and_modify(|stars| *stars += new_stars)
                .or_insert(new_stars);
        }
        self
    }
}

#[smartstream(aggregate)]
pub fn aggregate(accumulator: RecordData, current: &Record) -> Result<RecordData> {
    // Parse accumulator
    let accumulated_stars = serde_json::from_slice::<GithubStars>(accumulator.as_ref())
        .unwrap_or_else(|_| GithubStars::default());

    // Parse next record
    let new_stars = serde_json::from_slice::<GithubStars>(current.value.as_ref())?;

    // Add stars and serialize
    let summed_stars = accumulated_stars + new_stars;
    let summed_stars_bytes = serde_json::to_vec_pretty(&summed_stars)?;

    Ok(summed_stars_bytes.into())
}
