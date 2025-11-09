use indicatif::ProgressBar;

#[derive(Debug, Default)]
pub enum ProgressRenderer {
    /// Render the progress using eprintln macro
    #[default]
    Std,
    /// Render the progress using Indicatiff
    Indicatiff(ProgressBar),
}

impl ProgressRenderer {
    pub fn println(&self, msg: &str) {
        match self {
            ProgressRenderer::Std => println!("{msg}"),
            ProgressRenderer::Indicatiff(pb) => pb.suspend(|| println!("{msg}")),
        }
    }
}

impl From<ProgressBar> for ProgressRenderer {
    fn from(pb: ProgressBar) -> Self {
        Self::Indicatiff(pb)
    }
}
