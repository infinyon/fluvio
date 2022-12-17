#![warn(missing_docs)]

//! # Render text-based diagrams with Kroki
//!
//! This CLI takes in a text-based diagram, and uses a local host Kroki instance (by default)
//! to generate an output image and save it to disk.
//!
//! Use `--diff-check` in CI to only compare rendered output with existing diagram on disk
//!

use std::{path::PathBuf};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;
use std::process::exit;
use std::env::current_dir;

use clap::{Parser, ValueEnum};
use color_eyre::{Help, SectionExt};
use parse_display::{Display, FromStr};
use color_eyre::eyre::{eyre, Result, Context};
use tracing::{debug, error};
use serde::Deserialize;
use bytes::Bytes;

const KROKI_PUBLIC_URL: &str = "https://kroki.io";

#[derive(PartialEq, Debug, Clone, Default, Deserialize)]
struct DiagramBatch {
    #[serde(rename(deserialize = "diagram"))]
    pub diagrams: Vec<DiagramIo>,
}

#[derive(PartialEq, Debug, Clone, Default, Deserialize)]
struct DiagramIo {
    pub input_format: KrokiInputFormat,
    pub output_format: KrokiOutputFormat,
    pub source: PathBuf,
    pub destination: PathBuf,
}

// TODO: Maybe move fs::cannonize here
impl DiagramBatch {
    fn open_check(base: &Option<PathBuf>, config: &PathBuf) -> Result<Self> {
        let mut config = DiagramBatch::open(config)?;

        for d in config.diagrams.iter_mut() {
            d.source = resolve_path(base.as_ref(), &d.source)?;
            d.destination = resolve_path(base.as_ref(), &d.destination)?;
        }

        Ok(config)
    }

    fn open(config: &PathBuf) -> Result<Self> {
        let toml_data = fs::read_to_string(config)
            .with_section(|| format!("{}", config.display()).header("Config path:"))?;
        let files: DiagramBatch = toml::from_str(&toml_data)?;
        Ok(files)
    }
}

#[derive(Display, FromStr, PartialEq, Debug, Clone, Default, ValueEnum, Deserialize)]
#[display(style = "lowercase")]
#[serde(rename_all = "lowercase")]
enum KrokiOutputFormat {
    #[default]
    Svg,
    Jpg,
    Png,
    Pdf,
}

#[derive(Display, FromStr, PartialEq, Debug, Clone, Default, ValueEnum, Deserialize)]
#[display(style = "lowercase")]
#[serde(rename_all = "lowercase")]
enum KrokiInputFormat {
    Actdiag,
    Blockdiag,
    C4plantuml,
    Ditaa,
    Dot,
    Erd,
    #[default]
    Excalidraw,
    Graphviz,
    Nomnoml,
    Nwdiag,
    Plantuml,
    Seqdiag,
    Svgbob,
    Umlet,
    Vega,
    Vegalite,
}

#[derive(Parser, Debug)]
#[clap(author, version, about, long_about = None)]
struct Args {
    /// Path to the text-based diagram you want to render
    #[arg(long)]
    source: Option<PathBuf>,

    /// Path to output
    #[arg(long)]
    out_file: Option<PathBuf>,

    /// Base filesystem path for relative --source and --out-file paths
    #[arg(long)]
    base_path: Option<PathBuf>,

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
    // This is to support 1+ diagrams depending on entrypoint
    fn get_batch(&self) -> Result<DiagramBatch> {
        if self.source.is_none() && self.batch.is_none() {
            return Err(eyre!("`--source` or `--batch` required"));
        }

        let batch = if let Some(batch_config) = &self.batch {
            debug!(?batch_config, "Reading diagram list from config");

            // read file to string
            let file_batch =
                DiagramBatch::open_check(&self.base_path, batch_config).with_section(|| {
                    format!(
                        "{}",
                        current_dir()
                            .expect("Unable to get current directory:")
                            .display()
                    )
                    .header("Current working directory")
                })?;

            file_batch
        } else if let Some(source) = &self.source {
            debug!(?source, "Reading diagram from path");

            let diagram = DiagramIo {
                input_format: self.r#type.clone(),
                output_format: self.format.clone(),
                source: resolve_path(self.base_path.as_ref(), &source.clone())?,
                destination: resolve_path(
                    self.base_path.as_ref(),
                    &self
                        .out_file
                        .clone()
                        .expect("CLI parse should have caught this"),
                )?,
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
    color_eyre::install()?;

    let args = Args::parse();
    debug!(?args);

    let kroki_host = if !args.use_public {
        args.kroki_url.to_string()
    } else {
        KROKI_PUBLIC_URL.to_string()
    };

    // Check if Kroki instance is reachable
    let client = reqwest::blocking::Client::new();
    if client.get(&kroki_host).send()?.error_for_status().is_err() {
        return Err(eyre!("Unable to connect to Kroki host: {kroki_host}"));
    };

    debug!(?kroki_host);

    let batch = &args.get_batch()?;

    debug!(?batch);

    let mut err_found = false;

    for d in batch.diagrams.iter() {
        let diagram_data = fs::read_to_string(&d.source)
            .with_section(|| format!("{}", &d.source.display()).header("Source:"))
            .with_section(|| {
                format!(
                    "{:?}",
                    current_dir().expect("Unable to get current directory")
                )
                .header("Current directory:")
            })?;

        let input_format = d.input_format.clone().to_string();
        let output_format = d.output_format.clone().to_string();

        let postdata = HashMap::from([
            ("diagram_source", &diagram_data),
            ("diagram_type", &input_format),
            ("output_format", &output_format),
        ]);

        let res = client.post(&kroki_host).json(&postdata).send()?;

        if res.status().is_success() {
            let render = res.bytes()?;

            // This is our idempotency key
            let is_update = !is_render_match(d, &render)?;

            if args.diff_fail {
                // No file changes in this arm
                if is_update {
                    eprintln!(
                        "⛔ Render of {} and {} differ",
                        d.source.display(),
                        d.destination.display()
                    );
                    err_found = true;
                } else {
                    println!(
                        "✅ Render of {} and {} match",
                        d.source.display(),
                        d.destination.display()
                    );
                }
            } else {
                // File changes in this arm
                if is_update {
                    debug!("Writing response to file");
                    let mut output_file = File::create(&d.destination)
                        .with_section(|| {
                            format!("{}", &d.destination.display()).header("Destination:")
                        })
                        .with_section(|| {
                            format!(
                                "{:?}",
                                current_dir().expect("Unable to get current directory")
                            )
                            .header("Current directory:")
                        })?;
                    output_file.write_all(&render)?;
                    println!("✅ {} -> {}", d.source.display(), d.destination.display())
                } else {
                    println!(
                        "✅ Render of {} and {} match - No changes made",
                        d.source.display(),
                        d.destination.display()
                    );
                }
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

fn resolve_path(base: Option<&PathBuf>, path: &PathBuf) -> Result<PathBuf> {
    let p = if let Some(b) = base {
        if path.is_relative() {
            debug!("Path is relative");
            let abs_path = [b.to_path_buf().as_path(), path.to_path_buf().as_path()]
                .iter()
                .collect();
            debug!(?abs_path);
            abs_path
        } else {
            debug!("Path absolute - {}", path.display());
            path.to_path_buf()
        }
    } else {
        debug!("No base path. Using {}", path.display());
        path.to_path_buf()
    };

    fs::canonicalize(p)
        .with_context(|| format!("{base:#?}").header("base:"))
        .with_context(|| format!("{path:#?}").header("path:"))
}

fn is_render_match(diagram: &DiagramIo, render: &Bytes) -> Result<bool, color_eyre::Report> {
    match fs::read_to_string(&diagram.destination) {
        Ok(file) => {
            if file.as_bytes() == render {
                Ok(true)
            } else {
                Ok(false)
            }
        }
        Err(e) => {
            debug!(
                "Output file doesn't exist at expected location - {}",
                &diagram.destination.display()
            );
            eprintln!("⛔ {}", e);
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {

    use super::*;

    use std::io::Write;
    use std::iter;

    use tempfile::NamedTempFile;
    use rand::{Rng, thread_rng};
    use rand::distributions::Alphanumeric;
    use bytes::{BytesMut, BufMut};

    #[test]
    fn batch_config_parse() {
        let config = r#"
        # This is a toml comment

        [[diagram]]
        input_format = "excalidraw"
        output_format = "svg"
        source = "./my/source/diagram.excalidraw"
        destination = "./my/destination/diagram.svg"
        "#;

        let parsed: DiagramBatch = toml::from_str(config).unwrap();
        let diagram = &parsed.diagrams[0];

        assert_eq!(diagram.input_format, KrokiInputFormat::Excalidraw);
        assert_eq!(diagram.output_format, KrokiOutputFormat::Svg);
        assert_eq!(
            diagram.source,
            PathBuf::from("./my/source/diagram.excalidraw")
        );
        assert_eq!(
            diagram.destination,
            PathBuf::from("./my/destination/diagram.svg")
        );
    }

    #[test]
    fn file_match() {
        let mut rng = thread_rng();
        let write_1: String = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .map(char::from)
            .take(10)
            .collect();
        let write_2: String = iter::repeat(())
            .map(|()| rng.sample(Alphanumeric))
            .map(char::from)
            .take(10)
            .collect();

        let mut test_render = NamedTempFile::new().expect("Unable to create temp file");
        let mut compare = BytesMut::new();

        let test_diagram = DiagramIo {
            destination: test_render.path().to_path_buf(),
            ..Default::default()
        };

        // empty
        assert!(is_render_match(&test_diagram, &compare.to_vec().into()).unwrap());

        // Negative test
        test_render.write_all(write_1.clone().as_bytes()).unwrap();
        assert!(!is_render_match(&test_diagram, &compare.to_vec().into()).unwrap());

        // Positive test
        compare.put(write_1.as_bytes());
        assert!(is_render_match(&test_diagram, &compare.to_vec().into()).unwrap());

        // Negative test
        test_render.write_all(write_2.clone().as_bytes()).unwrap();
        assert!(!is_render_match(&test_diagram, &compare.to_vec().into()).unwrap());

        // Positive test
        compare.put(write_2.as_bytes());
        assert!(is_render_match(&test_diagram, &compare.to_vec().into()).unwrap());
    }
}
