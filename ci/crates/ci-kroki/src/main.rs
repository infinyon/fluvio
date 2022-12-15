#![warn(missing_docs)]

//! # Render text-based diagrams with Kroki
//!
//! This CLI takes in a text-based diagram, and uses a local host Kroki instance (by default)
//! to generate an output image and save it to disk.

use std::{path::PathBuf};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::process::exit;

use clap::{Parser, ValueEnum};
use parse_display::{Display, FromStr};
use color_eyre::eyre::{eyre, Result};
use tracing::{debug, error};
use serde::Deserialize;

const KROKI_PUBLIC_URL: &str = "https://kroki.io";

#[derive(PartialEq, Debug, Clone, Default, Deserialize)]
struct DiagramBatch {
    #[serde(rename(deserialize = "diagram"))]
    pub diagrams: Vec<DiagramIo>,
}

#[derive(PartialEq, Debug, Clone, Default, Deserialize)]
struct DiagramIo {
    pub source: PathBuf,
    pub destination: PathBuf,
}

impl DiagramBatch {
    fn open(config: &PathBuf) -> Result<Self> {
        let toml_data = fs::read_to_string(config)?;
        let files: DiagramBatch = toml::from_str(&toml_data)?;
        Ok(files)
    }
}

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
    #[arg(long)]
    source: Option<PathBuf>,

    /// Path to output
    #[arg(long, default_value = ".")]
    out_file: Option<PathBuf>,

    // Specify the format to render. Not all choices available for very input
    #[arg(long, default_value = "svg")]
    format: KrokiOutputFormat,

    /// Specify the type of diagram to render
    #[arg(long, default_value = "excalidraw")]
    r#type: KrokiInputFormat,

    /// Provide a config file for processing multiple diagrams. Overrides source/out-file if provided
    #[arg(long, conflicts_with_all = ["source", "out_file", "format", "type"])]
    batch: Option<PathBuf>,

    /// Specify the HTTP host for Kroki service
    #[arg(long, group = "kroki", default_value = "http://localhost:8000")]
    kroki_url: String,

    /// Use the public Kroki service at https://kroki.io
    #[arg(long, group = "kroki", action)]
    use_public: bool,

    /// Report as failure if rendered diagram differs from existing output diagram
    #[arg(long, action)]
    diff_fail: bool,
}

impl Args {
    fn get_batch(&self) -> Result<DiagramBatch> {
        if self.source.is_none() && self.batch.is_none() {
            return Err(eyre!("`--source` or `--batch` required"));
        }

        let batch = if let Some(batch_config) = &self.batch {
            debug!(?batch_config, "Reading diagram list from config");

            // read file to string
            DiagramBatch::open(batch_config)?
        } else if let Some(source) = &self.source {
            debug!(?source, "Reading diagram from path");

            let diagram = DiagramIo {
                source: source.clone(),
                destination: self
                    .out_file
                    .clone()
                    .expect("CLI parse should have caught this"),
            };

            DiagramBatch {
                diagrams: vec![diagram],
            }
        } else {
            unreachable!()
        };

        Ok(batch)
    }
}

fn main() -> Result<()> {
    fluvio_future::subscriber::init_tracer(None);
    color_eyre::config::HookBuilder::blank();

    let args = Args::parse();
    debug!(?args);

    // Check if Kroki instance is reachable
    let kroki_host = if !args.use_public {
        args.kroki_url.to_string()
    } else {
        KROKI_PUBLIC_URL.to_string()
    };

    let client = reqwest::blocking::Client::new();
    if client.get(&kroki_host).send()?.error_for_status().is_err() {
        return Err(eyre!("Unable to connect to Kroki host: {kroki_host}"));
    };

    debug!(?kroki_host);

    let batch = &args.get_batch()?;

    debug!(?batch);

    let mut err_found = false;

    for d in batch.diagrams.iter() {
        // Read in source file name w/o the extension
        let input_filestem = &d
            .source
            .file_stem()
            .ok_or_else(|| eyre!("Unable to read file-stem from input path"))?
            .to_str()
            .ok_or_else(|| eyre!("Unable to convert input file-stem to str"))?
            .to_string();

        debug!(?input_filestem);

        let input_extension = &d
            .source
            .extension()
            .ok_or_else(|| eyre!("Unable to read extension from input path"))?
            .to_str()
            .ok_or_else(|| eyre!("Unable to convert extension to str"))?
            .to_string();

        debug!(?input_extension);

        let output_filestem = &d.destination.file_stem();
        debug!(?output_filestem);

        let output_extension = &d.destination.extension();
        debug!(?output_extension);

        let diagram_data = fs::read_to_string(&d.source)?;

        let input_type_str = args.r#type.to_string();

        let (output_path_str, output_format_str) = match (output_filestem, output_extension) {
            (Some(_), Some(exe)) => (
                d.destination.display().to_string(),
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

        let res = client.post(&kroki_host).json(&postdata).send()?;

        if res.status().is_success() {
            if args.diff_fail {
                debug!("Compare output with filesystem");
                match fs::read_to_string(&output_path_str) {
                    Ok(file) => {
                        if file.as_bytes() == res.bytes()? {
                            println!(
                                "✅ Render of {} and {output_path_str} match",
                                d.source.display()
                            );
                        } else {
                            err_found = true;
                            eprintln!(
                                "⛔ Render of {} and {output_path_str} differ",
                                d.source.display()
                            );
                        }
                    }
                    Err(e) => {
                        debug!("Output file doesn't exist at expected location");
                        err_found = true;
                        eprintln!("⛔ {}", e)
                    }
                }
            } else {
                debug!("Writing response to file");
                let mut output_file = File::create(&output_path_str)?;
                output_file.write_all(&res.bytes()?)?;
                println!("✅ {} -> {output_path_str}", d.source.display())
            }
        } else {
            error!("There was an error creating image");
            err_found = true;
            eprintln!("⛔ There was an error creating image");
            eprintln!("{:#?}", res.text()?);
        }
    }

    if err_found {
        eprintln!("❌ Fail");
        exit(1)
    } else {
        println!("✅ Success")
    }

    Ok(())
}
