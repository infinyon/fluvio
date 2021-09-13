pub trait ProgressRenderedText {
    /// Rendered text of the last finished step in the progress
    fn text(&self) -> String;
    /// Rendered text of the current step in the progress
    fn next_step_text(&self) -> Option<String> {
        None
    }
}
