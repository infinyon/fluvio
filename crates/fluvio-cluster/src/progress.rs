use std::borrow::Cow;

use indicatif::{ProgressBar, ProgressStyle};

use crate::render::{ProgressRenderedText, ProgressRenderer};

#[derive(Debug)]
pub(crate) enum InstallProgressMessage {
    PreFlightCheck,
    LaunchingSC,
    ScLaunched,

    StartSPU(u16, u16),
    ProfileSet,
    Success,
}

impl ProgressRenderedText for InstallProgressMessage {
    fn msg(&self) -> String {
        use colored::*;

        match self {
            InstallProgressMessage::PreFlightCheck => {
                format!("{}", "📝 Running pre-flight checks".bold())
            }

            InstallProgressMessage::LaunchingSC => {
                format!("🖥️  {}", "Starting SC server".bold())
            }

            InstallProgressMessage::ScLaunched => {
                format!("✅ {}", "SC Launched".bold())
            }

            InstallProgressMessage::StartSPU(spu_num, total) => {
                format!("{} ({}/{})", "🤖 Starting SPU:", spu_num, total)
            }

            InstallProgressMessage::ProfileSet => {
                format!("👤 {}", "Profile set".bold())
            }
            InstallProgressMessage::Success => {
                format!("🎯 {}", "Successfully installed Fluvio!".bold())
            }
        }
    }
}

fn create_spinning_indicator() -> ProgressBar {
    let pb = ProgressBar::new(1);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg} {spinner}")
            .tick_chars("/-\\|"),
    );
    pb.enable_steady_tick(100);
    pb
}

#[derive(Debug)]
pub struct ProgressBarFactory {
    hide: bool,
    plain: ProgressRenderer,
}

impl ProgressBarFactory {
    pub fn new(hide: bool) -> Self {
        Self {
            hide,
            plain: Default::default(),
        }
    }

    /// create new progress bar
    pub fn create(&self) -> ProgressRenderer {
        if self.hide || std::env::var("CI").is_ok() {
            Default::default()
        } else {
            create_spinning_indicator().into()
        }
    }

    /// simple print
    pub fn println(&self, msg: impl Into<Cow<'static, str>>) {
        self.plain.println(msg);
    }
}
