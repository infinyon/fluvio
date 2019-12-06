//!
//! # Root CLI
//!
//! CLI configurations at the top of the tree
//!

use std::io::Error as IoError;

use structopt::clap::AppSettings;
use structopt::StructOpt;

use kf_protocol_build::format_code::TemplateFormat;

use kf_protocol_build::check_keys::check_header_and_field_keys_are_known;
use kf_protocol_build::generate_code::gen_code_and_output_to_dir;

macro_rules! print_cli_err {
    ($x:expr) => {
        println!("\x1B[1;31merror:\x1B[0m {}", $x);
    };
}

#[derive(StructOpt)]
#[structopt(
    about = "KafkaSpec2Code CLI",
    author = "",
    raw(
        global_settings = "&[AppSettings::DisableVersion, AppSettings::VersionlessSubcommands, AppSettings::DeriveDisplayOrder]"
    )
)]

enum Root {
    #[structopt(
        name = "generate",
        author = "",
        about = "Generate Rust APIs from Kafka json specs"
    )]
    Generate(GenerateOpt),

    #[structopt(
        name = "check-keys",
        author = "",
        about = "Check if any new keys have been introduced"
    )]
    CheckKeys(CheckKeysOpt),
}

fn run_cli() {
    let result = match Root::from_args() {
        Root::Generate(generate) => process_code_generator(generate),
        Root::CheckKeys(check_keys) => process_check_keys(check_keys),
    };

    if result.is_err() {
        print_cli_err!(result.unwrap_err());
    }
}

#[derive(StructOpt)]
pub struct GenerateOpt {
    /// Input directory
    #[structopt(short = "i", long = "input", value_name = "string")]
    pub input_dir: String,

    /// Directory to output each file pair
    #[structopt(short = "d", long = "output-directory", value_name = "string")]
    pub output_dir: String,

    /// Skip Rust formatter (useful for troubleshooting custom templates)
    #[structopt(short = "s", long = "skip-formatter")]
    pub skip_formatter: bool,

    /// Use custom template
    #[structopt(short = "t", long = "custom-template", value_name = "string")]
    pub template_dir: Option<String>,
}

#[derive(StructOpt)]
pub struct CheckKeysOpt {
    /// Input directory
    #[structopt(short = "i", long = "input", value_name = "string")]
    pub input_dir: String,
}

// -----------------------------------
// Code Generator
// -----------------------------------

/// Process code generator
pub fn process_code_generator(opt: GenerateOpt) -> Result<(), IoError> {
    let template = TemplateFormat::new(opt.template_dir)?;
    gen_code_and_output_to_dir(
        &opt.input_dir,
        &opt.output_dir,
        &template,
        opt.skip_formatter,
    )
}

// -----------------------------------
// Check Keys
// -----------------------------------

/// Process check keys
pub fn process_check_keys(opt: CheckKeysOpt) -> Result<(), IoError> {
    check_header_and_field_keys_are_known(&opt.input_dir)?;
    println!("All fields are known.");

    Ok(())
}

fn main() {
    run_cli();
}
