use std::borrow::Cow;

use indicatif::{ProgressBar, ProgressStyle};

use crate::render::{ProgressRenderedText, ProgressRenderer};

#[derive(Debug)]
pub(crate) enum InstallProgressMessage {
    PreFlightCheck,
    LaunchingSC,
    ScLaunched,
    ConnectingSC,

    StartSPU(u16, u16),
    WaitingForSPU(usize, usize),
    SpuGroupLaunched(u16),
    InstallingChart,
    UpgradingChart,
    ChartInstalled,
    FoundSC(String),
    ConfirmingSpus,
    SpusConfirmed,
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
            InstallProgressMessage::ChartInstalled => {
                format!("{:>6} {}", "✅", "Fluvio app chart has been installed")
            }
            InstallProgressMessage::LaunchingSC => {
                format!("🖥️  {}", "Starting SC server".bold())
            }
            InstallProgressMessage::FoundSC(address) => {
                format!("🔎 {} {}", "Found SC service addr:".bold(), address.bold())
            }
            InstallProgressMessage::ScLaunched => {
                format!("✅ {}", "SC Launched".bold())
            }
            InstallProgressMessage::InstallingChart => {
                format!("{:>6} {}", "📊", "Installing Fluvio chart")
            }
            InstallProgressMessage::UpgradingChart => {
                format!("{:>6} {}", "📊", "Upgrading Fluvio chart")
            }
            InstallProgressMessage::ConnectingSC => {
                format!("🔗 {}", "Trying to connect to SC".bold())
            }

            InstallProgressMessage::WaitingForSPU(spu_num, total) => {
                format!("{:>6} ({}/{})", "🤖 Waiting for SPU:", spu_num, total)
            }

            InstallProgressMessage::StartSPU(spu_num, total) => {
                format!("{} ({}/{})", "🤖 Starting SPU:", spu_num, total)
            }
            InstallProgressMessage::SpuGroupLaunched(spu_num) => {
                format!("🤖 {} ({})", "SPU group launched".bold(), spu_num)
            }
            InstallProgressMessage::ConfirmingSpus => {
                format!("💙 {}", "Confirming SPUs".bold())
            }
            InstallProgressMessage::SpusConfirmed => {
                format!("{:>6} {}", "✅", "All SPUs confirmed")
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
