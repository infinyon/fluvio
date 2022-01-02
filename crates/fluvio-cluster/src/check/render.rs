#![allow(unused)]

use futures_util::StreamExt;
use async_channel::Receiver;
use crate::{
    CheckResult, CheckResults, CheckStatus, CheckSuggestion,
    render::{ProgressRenderedText, ProgressRenderer},
};

const ISSUE_URL: &str = "https://github.com/infinyon/fluvio/issues/new/choose";
