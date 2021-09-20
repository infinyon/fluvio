use indicatif::{ProgressBar, ProgressStyle};

use crate::render::ProgressRenderedText;

#[derive(Debug)]
pub(crate) enum InstallProgressMessage {
    PreFlightCheck,
    LaunchingSC,
    ScLaunched,
    LaunchingSPUGroup(u16),
    StartSPU(u16, u16),
    SpuGroupLaunched(u16),
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
            InstallProgressMessage::LaunchingSC => {
                format!("🖥️  {}", "Starting SC server".bold())
            }

            InstallProgressMessage::ScLaunched => {
                format!("🖥️  {}", "SC Launched".bold())
            }
            InstallProgressMessage::LaunchingSPUGroup(spu_num) => {
                format!("{} {}", "🤖 Launching SPU Group with:".bold(), spu_num)
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
                format!("{:>6} {}", "✅".bold().green(), "All SPUs confirmed")
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

pub(crate) fn create_progress_indicator() -> ProgressBar {
    let pb = ProgressBar::new(1);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{msg} {spinner}")
            .tick_chars("/-\\|"),
    );
    pb.enable_steady_tick(100);
    pb
}
