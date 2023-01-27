use std::borrow::Cow;

use indicatif::ProgressBar;

pub trait ProgressRenderedText {
    /// Rendered text of the last finished step in the progress
    fn msg(&self) -> String;
}

#[derive(Debug)]
pub enum ProgressRenderer {
    /// Render the progress using eprintln macro
    Std,
    /// Render the progress using Indicatiff
    Indicatiff(ProgressBar),
}

impl ProgressRenderer {
    pub fn println(&self, msg: impl Into<Cow<'static, str>>) {
        match self {
            ProgressRenderer::Std => eprintln!("{}", msg.into()),
            ProgressRenderer::Indicatiff(pb) => pb.println(msg.into()),
        }
    }

    pub fn set_message(&self, msg: impl Into<Cow<'static, str>>) {
        let msg = msg.into();
        match self {
            ProgressRenderer::Std => eprintln!("{msg}"),
            ProgressRenderer::Indicatiff(pb) => pb.set_message(msg),
        }
    }

    pub fn finish_and_clear(&self) {
        if let ProgressRenderer::Indicatiff(pb) = self {
            pb.finish_and_clear();
        }
    }
}

impl From<ProgressBar> for ProgressRenderer {
    fn from(pb: ProgressBar) -> Self {
        Self::Indicatiff(pb)
    }
}

impl Default for ProgressRenderer {
    fn default() -> Self {
        Self::Std
    }
}
