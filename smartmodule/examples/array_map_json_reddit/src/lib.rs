//! An ArrayMap for breaking apart a paginated Reddit API response into individual posts.
//!
//! The SmartModules ArrayMap function allows you map a single input Record into
//! zero or many output records. This example showcases taking a stream of Reddit API
//! responses and converting it into a stream of the individual posts.

use fluvio_smartmodule::{smartmodule, Record, RecordData, Result};
use serde::{Serialize, Deserialize};

#[derive(Debug, Serialize, Deserialize)]
struct RedditListing {
    data: RedditPage,
}

#[derive(Debug, Serialize, Deserialize)]
struct RedditPage {
    children: Vec<RedditPost>,
}

#[derive(Debug, Serialize, Deserialize)]
struct RedditPost {
    data: RedditPostData,
}

#[derive(Debug, Serialize, Deserialize)]
struct RedditPostData {
    id: String,
    title: String,
    url: String,
    selftext: String,
    ups: i32,
    upvote_ratio: f32,
}

#[smartmodule(array_map)]
pub fn array_map(record: &Record) -> Result<Vec<(Option<RecordData>, RecordData)>> {
    // Deserialize a RedditListing from JSON
    let listing: RedditListing = serde_json::from_slice(record.value.as_ref())?;

    // Create a list of RedditPostData converted back into JSON strings
    let posts: Vec<(String, String)> = listing
        .data
        .children
        .into_iter()
        .map(|post: RedditPost| {
            // Convert each post into (ID, Post JSON)
            serde_json::to_string(&post.data).map(|json| (post.data.id, json))
        })
        .collect::<core::result::Result<_, _>>()?;

    // Convert each Post into a Record whose key is the Post's ID
    let records = posts
        .into_iter()
        .map(|(id, post)| (Some(RecordData::from(id)), RecordData::from(post)))
        .collect();
    Ok(records)
}
