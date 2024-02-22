use std::{borrow::Cow, time::Duration};

use indicatif::{ProgressBar, ProgressStyle};

use crate::render::{ProgressRenderedText, ProgressRenderer};

use anyhow::Result;

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

fn create_spinning_indicator() -> Result<ProgressBar> {
    let pb = ProgressBar::new(1);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg} {spinner}")?
            .tick_chars("/-\\|"),
    );
    pb.enable_steady_tick(Duration::from_millis(100));
    Ok(pb)
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
    pub fn create(&self) -> Result<ProgressRenderer> {
        if self.hide || std::env::var("CI").is_ok() {
            Ok(Default::default())
        } else {
            Ok(create_spinning_indicator()?.into())
        }
    }

    /// simple print
    pub fn println(&self, msg: impl Into<Cow<'static, str>>) {
        self.plain.println(msg);
    }
}
