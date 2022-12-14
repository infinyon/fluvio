use std::{path::PathBuf};
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::Write;

#[warn(missing_docs)]
use clap::Parser;
use parse_display::{Display, FromStr};
use color_eyre::eyre::{eyre, Result};

//#[derive(PartialEq, Debug, Clone)]
//struct KrokiPostData {
//    diagram_source: String,
//    diagram_type: KrokiInputFormat,
//    output_format: KrokiOutputFormat,
//}
#[derive(Display, FromStr, PartialEq, Debug, Clone, Default)]
#[display(style = "lowercase")]
enum KrokiOutputFormat {
    #[default]
    Svg,
    Jpg,
    Png,
    Pdf,
}

#[derive(Display, FromStr, PartialEq, Debug, Clone, Default)]
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

/// Simple program to greet a person
#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    ///
    source: PathBuf,

    ///
    #[arg(long, default_value = ".")]
    out_file: PathBuf,

    //
    //#[arg(long, default_value = "svg")]
    #[arg(long, default_value = "svg")]
    format: KrokiOutputFormat,

    #[arg(long, default_value = "excalidraw")]
    r#type: KrokiInputFormat,

    ///
    //#[arg(long)]
    //config: PathBuf,

    ///
    #[arg(long, group = "kroki", default_value = "http://localhost:8000")]
    kroki_url: String,

    ///
    #[arg(long, group = "kroki", action)]
    use_public: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();

    // Read in source file name w/o the extension
    let input_filestem = &args
        .source
        .file_stem()
        .ok_or_else(|| eyre!("Unable to read file-stem from input path"))?
        .to_str()
        .ok_or_else(|| eyre!("Unable to convert input file-stem to str"))?
        .to_string();

    let _input_extension = &args
        .source
        .extension()
        .ok_or_else(|| eyre!("Unable to read extension from input path"))?
        .to_str()
        .ok_or_else(|| eyre!("Unable to convert extension to str"))?
        .to_string();

    let diagram_data = fs::read_to_string(args.source)?.parse()?;

    let input_type_str = args.r#type.to_string();
    let output_path_str = args.out_file.display().to_string(); // TODO: Parse this for a filename and handle if not provided
    let output_format_str = args.format.to_string();

    let postdata = HashMap::from([
        ("diagram_source", &diagram_data),
        ("diagram_type", &input_type_str),
        ("output_format", &output_format_str),
    ]);

    let client = reqwest::blocking::Client::new();
    let res = client.post(args.kroki_url).json(&postdata).send()?;

    if res.status().is_success() {
        let output_filename = format!("{output_path_str}/{input_filestem}.{output_format_str}",);

        let mut output_file = File::create(output_filename)?;

        output_file.write(&res.bytes()?)?;
    } else {
        eprintln!("There was an error creating image");
        eprintln!("{:#?}", res.text()?);
    }

    Ok(())
}
