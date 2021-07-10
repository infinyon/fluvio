use std::collections::HashMap;
use fluvio_smartstream::{smartstream, Record, RecordData};
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize)]
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
pub fn aggregate(accumulator: RecordData, next: &Record) -> RecordData {
    // Parse accumulator
    let accumulated_stars = serde_json::from_slice::<GithubStars>(accumulator.as_ref()).unwrap();

    // Parse next record
    let new_stars = serde_json::from_slice::<GithubStars>(next.value.as_ref()).unwrap();

    // Add stars and serialize
    let summed_stars = accumulated_stars + new_stars;
    let summed_stars_bytes = serde_json::to_vec_pretty(&summed_stars).unwrap();

    summed_stars_bytes.into()
}
