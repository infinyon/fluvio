use std::io::{Error, ErrorKind};
use crossbeam_channel::{bounded, select, Receiver};

use fluvio_cdc::producer::{Config, get_cli_opt};
use fluvio_cdc::producer::{FluvioManager, BinLogManager, Resume};
use fluvio_cdc::messages::BinLogMessage;
use fluvio_cdc::error::CdcError;

async fn run() -> Result<(), CdcError> {
    // read profile
    let params = get_cli_opt();
    let config = Config::load(&params.profile)?;
    let profile = config.profile();

    // create channels
    let ctrl_c_events = ctrl_channel()?;
    let (sender, receiver) = bounded::<String>(100);

    // create fluvio manager
    let mut flv_manager = FluvioManager::new(profile.topic(), profile.replicas(), None).await?;

    // create binlog manager
    let bn_manager = BinLogManager::new(&profile, sender)?;

    // create resume
    println!("Using resume file path: {:?}", profile.resume_file());
    let mut resume = Resume::load(profile.resume_file()).await?;
    if let Some(binfile) = resume.binfile.as_ref() {
        println!("Resuming with {:?}", binfile);
    }

    let ts_frequency = None;
    bn_manager.run(resume.clone(), ts_frequency);

    loop {
        select! {
            recv(receiver) -> msg => {
                match msg {
                    Ok(msg) => {
                        let bn_message: BinLogMessage = serde_json::from_str(&msg)?;
                        let bn_file = bn_message.bn_file.clone();
                        if let Err(err) = flv_manager.process_msg(bn_message).await {
                            println!("{:?}", err);
                            std::process::exit(0);
                        }
                        resume.update_binfile(bn_file).await?;
                    },
                    Err(err) => {
                        println!("{:?}", err);
                        std::process::exit(0);
                    }
                }
            }
            recv(ctrl_c_events) -> _ => {
                println!();
                println!("Exited by user");
                break;
            }
        }
    }
    Ok(())
}

fn ctrl_channel() -> Result<Receiver<()>, Error> {
    let (sender, receiver) = bounded(100);
    if let Err(err) = ctrlc::set_handler(move || {
        let _ = sender.send(());
    }) {
        return Err(Error::new(ErrorKind::InvalidInput, err));
    }

    Ok(receiver)
}

fn main() {
    if let Err(err) = async_std::task::block_on(run()) {
        println!("Error: {:?}", err);
    }
}
