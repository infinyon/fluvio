use std::collections::HashMap;
use fluvio_smartmodule::{smartmodule, Result, Record, RecordData};
use serde::{Serialize, Deserialize};

#[derive(Default, Serialize, Deserialize)]
struct GithubStars(HashMap<String, u32>);

impl std::ops::Add for GithubStars {
    type Output = Self;

    fn add(mut self, next: Self) -> Self::Output {
        for (repo, new_stars) in next.0 {
            self.0
                .entry(repo)
                .and_modify(|stars| *stars += new_stars)
                .or_insert(new_stars);
        }
        self
    }
}

#[smartmodule(aggregate)]
pub fn aggregate(accumulator: RecordData, current: &Record) -> Result<RecordData> {
    // Parse accumulator
    let accumulated_stars: GithubStars =
        serde_json::from_slice(accumulator.as_ref()).unwrap_or_default();

    // Parse next record
    let new_stars: GithubStars = serde_json::from_slice(current.value.as_ref())?;

    // Add stars and serialize
    let summed_stars = accumulated_stars + new_stars;
    let summed_stars_bytes = serde_json::to_vec_pretty(&summed_stars)?;

    Ok(summed_stars_bytes.into())
}
