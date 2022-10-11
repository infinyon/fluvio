use clap::Parser;
use color_eyre::eyre::Result;
use fluvio_cli::{Root, HelpOpt};
use fluvio_future::task::run_block_on;

fn main() -> Result<()> {
    fluvio_future::subscriber::init_tracer(None);
    color_eyre::config::HookBuilder::blank()
        .display_env_section(false)
        .install()?;
    print_help_hack()?;
    let root: Root = Root::parse();

    // If the CLI comes back with an error, attempt to handle it
    if let Err(e) = run_block_on(root.process()) {
        let user_error = e.get_user_error()?;
        eprintln!("{}", user_error);
        std::process::exit(1);
    }

    Ok(())
}

fn print_help_hack() -> Result<()> {
    let mut args = std::env::args();
    if args.len() < 2 {
        HelpOpt {}.process()?;
        std::process::exit(0);
    } else if let Some(first_arg) = args.nth(1) {
        // We pick help up here as a courtesy
        if vec!["-h", "--help", "help"].contains(&first_arg.as_str()) {
            HelpOpt {}.process()?;
            std::process::exit(0);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use clap::Parser;
    use fluvio_cli::Root;

    #[test]
    fn test_correct_command_parsing_help() {
        let should_not_succeed = vec![
            "fluvio",
            "fluvio -h",
            "fluvio --help", // fluvio help is hacked with print_help_hack
        ];
        for s in should_not_succeed {
            assert!(!parse_succeeds(s), "{s}");
        }
    }

    #[test]
    fn test_correct_command_parsing_consume() {
        let should_succeed = vec![
            "fluvio consume -B hello",
            "fluvio consume -T hello",
            "fluvio consume -B -n 10 hello",
            "fluvio consume -T -n 10 hello",
        ];
        for s in should_succeed {
            assert!(parse_succeeds(s), "{s}");
        }

        let should_not_succeed = vec![
            "fluvio consume",
            "fluvio consume -B 0 hello",
            "fluvio consume -T 0 hello",
            "fluvio consume -B -n -10 hello",
            "fluvio consume -B -n hello",
            "fluvio consume -n 10 hello",
            "fluvio consume -B -T -n 10 hello",
            "fluvio consume -n hello",
        ];
        for s in should_not_succeed {
            assert!(!parse_succeeds(s), "{s}");
        }
    }

    fn parse_succeeds(command: &str) -> bool {
        Root::try_parse_from(command.split_whitespace()).is_ok()
    }
}
