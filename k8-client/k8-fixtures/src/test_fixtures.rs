use rand::prelude::*;
use std::env;
use std::ffi::OsStr;
use std::fs::File;
use std::io::Read;
use std::path::{Path, PathBuf};

use k8_metadata::core::metadata::K8Watch;
use k8_client::TokenStreamResult;
use k8_metadata::topic::{TopicSpec, TopicStatus};

//
// Topic Watch Fixtures
//

pub type TestTopicWatchList = Vec<TestTopicWatch>;

pub struct TestTopicWatch {
    pub operation: String,
    pub name: String,
    pub partitions: i32,
    pub replication: i32,
    pub ignore_rack_assignment: Option<bool>,
}

pub fn create_topic_watch(ttw: &TestTopicWatch) -> K8Watch<TopicSpec, TopicStatus> {
    let target_dir = get_target_dir();
    let path = get_top_dir(&target_dir);
    let mut contents = String::new();
    let (filename, file_has_options) = if ttw.ignore_rack_assignment.is_none() {
        (
            String::from("k8-client/k8-fixtures/data/topic_no_options.tmpl"),
            false,
        )
    } else {
        (
            String::from("k8-client/k8-fixtures/data/topic_all.tmpl"),
            true,
        )
    };
    let f = File::open(path.join(filename));
    f.unwrap().read_to_string(&mut contents).unwrap();

    contents = contents.replace("{type}", &*ttw.operation);
    contents = contents.replace("{name}", &*ttw.name);
    contents = contents.replace("{partitions}", &*ttw.partitions.to_string());
    contents = contents.replace("{replication}", &*ttw.replication.to_string());
    contents = contents.replace(
        "{12_digit_rand}",
        &*format!("{:012}", thread_rng().gen_range(0, 999999)),
    );
    if file_has_options {
        contents = contents.replace(
            "{rack_assignment}",
            &*ttw.ignore_rack_assignment.unwrap().to_string(),
        );
    }
    serde_json::from_str(&contents).unwrap()
}

pub fn create_topic_stream_result(
    ttw_list: &TestTopicWatchList,
) -> TokenStreamResult<TopicSpec, TopicStatus> {
    let mut topic_watch_list = vec![];
    for ttw in ttw_list {
        topic_watch_list.push(Ok(create_topic_watch(&ttw)));
    }
    Ok(topic_watch_list)
}

//
// Utility APIs
//

// Get absolute path to the "target" directory ("build" dir)
fn get_target_dir() -> PathBuf {
    let bin = env::current_exe().expect("exe path");
    let mut target_dir = PathBuf::from(bin.parent().expect("bin parent"));
    while target_dir.file_name() != Some(OsStr::new("target")) {
        target_dir.pop();
    }
    target_dir
}

// Get absolute path to the project's top dir, given target dir
fn get_top_dir<'a>(target_dir: &'a Path) -> &'a Path {
    target_dir.parent().expect("target parent")
}
