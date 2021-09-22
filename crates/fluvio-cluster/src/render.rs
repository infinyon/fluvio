pub trait ProgressRenderedText {
    /// Rendered text of the last finished step in the progress
    fn msg(&self) -> String;
}
