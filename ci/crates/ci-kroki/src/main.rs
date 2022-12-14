#![warn(missing_docs)]

//! # Render text-based diagrams with Kroki
//!
//! This CLI takes in a text-based diagram, and uses a local host Kroki instance (by default)
//! to generate an output image and save it to disk.

use std::{path::PathBuf};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;

use clap::{Parser, ValueEnum};
use parse_display::{Display, FromStr};
use color_eyre::eyre::{eyre, Result};
use tracing::{debug, error};

const KROKI_PUBLIC_URL: &str = "https://kroki.io";

#[derive(Display, FromStr, PartialEq, Debug, Clone, Default, ValueEnum)]
#[display(style = "lowercase")]
enum KrokiOutputFormat {
    #[default]
    Svg,
    Jpg,
    Png,
    Pdf,
}

#[derive(Display, FromStr, PartialEq, Debug, Clone, Default, ValueEnum)]
#[display(style = "lowercase")]
enum KrokiInputFormat {
    //Actdiag,
    //Blockdiag,
    //C4plantuml,
    //Ditaa,
    //Dot,
    //Erd,
    #[default]
    Excalidraw,
    //Graphviz,
    //Nomnoml,
    //Nwdiag,
    //Plantuml,
    //Seqdiag,
    //Svgbob,
    //Umlet,
    //Vega,
    //Vegalite
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to the text-based diagram you want to render
    source: PathBuf,

    /// Path to output
    #[arg(long, default_value = ".")]
    out_file: PathBuf,

    // Specify the format to render. Not all choices available for very input
    #[arg(long, default_value = "svg")]
    format: KrokiOutputFormat,

    /// Specify the type of diagram to render
    #[arg(long, default_value = "excalidraw")]
    r#type: KrokiInputFormat,

    ///
    //#[arg(long)]
    //config: PathBuf,

    /// Specify the HTTP host for Kroki service
    #[arg(long, group = "kroki", default_value = "http://localhost:8000")]
    kroki_url: String,

    /// Use the public Kroki service at https://kroki.io
    #[arg(long, group = "kroki", action)]
    use_public: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    debug!(?args);

    fluvio_future::subscriber::init_tracer(None);
    color_eyre::config::HookBuilder::blank();

    // Read in source file name w/o the extension
    let input_filestem = &args
        .source
        .file_stem()
        .ok_or_else(|| eyre!("Unable to read file-stem from input path"))?
        .to_str()
        .ok_or_else(|| eyre!("Unable to convert input file-stem to str"))?
        .to_string();

    debug!(?input_filestem);

    let input_extension = &args
        .source
        .extension()
        .ok_or_else(|| eyre!("Unable to read extension from input path"))?
        .to_str()
        .ok_or_else(|| eyre!("Unable to convert extension to str"))?
        .to_string();

    debug!(?input_extension);

    let output_filestem = &args.out_file.file_stem();
    debug!(?output_filestem);

    let output_extension = &args.out_file.extension();
    debug!(?output_extension);

    let diagram_data = fs::read_to_string(args.source)?.parse()?;

    let input_type_str = args.r#type.to_string();

    let (output_path_str, output_format_str) = match (output_filestem, output_extension) {
        (Some(_), Some(exe)) => (
            args.out_file.display().to_string(),
            exe.to_str()
                .ok_or_else(|| eyre!("Unable to convert output file-stem to str"))?
                .to_string(),
        ),
        // Default to args.format if not part of filename
        (Some(stem), None) => (
            format!(
                "{}.{}",
                stem.to_str()
                    .ok_or_else(|| eyre!("Unable to convert output file-stem to str"))?,
                args.format
            ),
            args.format.to_string(),
        ),
        // Use input file name, and args.format if we only have a path
        _ => (
            format!("{}.{}", input_filestem, args.format),
            args.format.to_string(),
        ),
    };

    debug!(?output_path_str);
    debug!(?output_format_str);

    let postdata = HashMap::from([
        ("diagram_source", &diagram_data),
        ("diagram_type", &input_type_str),
        ("output_format", &output_format_str),
    ]);

    let kroki_host = if !args.use_public {
        args.kroki_url
    } else {
        KROKI_PUBLIC_URL.to_string()
    };

    debug!(?kroki_host);

    let client = reqwest::blocking::Client::new();
    let res = client.post(kroki_host).json(&postdata).send()?;

    if res.status().is_success() {
        debug!("Writing response to file");
        let mut output_file = File::create(output_path_str)?;
        output_file.write_all(&res.bytes()?)?;
    } else {
        error!("There was an error creating image");
        eprintln!("There was an error creating image");
        eprintln!("{:#?}", res.text()?);
    }

    Ok(())
}
