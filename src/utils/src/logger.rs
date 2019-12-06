use chrono::Local;
use env_logger::{
    fmt::{Color, Style, StyledValue},
    Builder,
};
use log::Level;
use std::sync::atomic::{AtomicUsize, Ordering};

static MAX_MODULE_WIDTH: AtomicUsize = AtomicUsize::new(0);

fn colored_level<'a>(style: &'a mut Style, level: Level) -> StyledValue<'a, &'static str> {
    match level {
        Level::Trace => style.set_color(Color::Magenta).value("TRACE"),
        Level::Debug => style.set_color(Color::Blue).value("DEBUG"),
        Level::Info => style.set_color(Color::Green).value("INFO "),
        Level::Warn => style.set_color(Color::Yellow).value("WARN "),
        Level::Error => style.set_color(Color::Red).value("ERROR"),
    }
}

pub fn init_logger() {
    let mut builder = Builder::from_default_env();

    builder.format(|f, record| {
        use std::io::Write;
        let target = record.target();
        let mut max_width = MAX_MODULE_WIDTH.load(Ordering::Relaxed);
        if max_width < target.len() {
            MAX_MODULE_WIDTH.store(target.len(), Ordering::Relaxed);
            max_width = target.len();
        }

        let mut style = f.style();
        let level = colored_level(&mut style, record.level());
        let mut style = f.style();
        let target = style
            .set_bold(true)
            .value(format!("{: <width$}", target, width = max_width));
        writeln!(
            f,
            "{} {} {} > {}",
            //Local::now().format("%Y-%m-%d %H:%M:%S%.3f"),
            Local::now().format("%H:%M:%S%.3f"),
            level,
            target,
            record.args(),
        )
    });

    let _ = builder.try_init();
}
