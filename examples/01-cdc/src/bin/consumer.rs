use std::io::{Error, ErrorKind};
use crossbeam_channel::{bounded, select, Receiver, Sender};

use fluvio_cdc::consumer::{Config, get_cli_opt};
use fluvio_cdc::consumer::MysqlManager;
use fluvio_cdc::offset_store::OffsetStore;

use fluvio::{FluvioError, PartitionConsumer, Offset};

async fn run() -> Result<(), FluvioError> {
    // read profile
    let params = get_cli_opt();
    let config = Config::load(&params.profile)?;
    let profile = config.profile();

    // init store
    let mut offset_store = OffsetStore::init(profile.last_offset_file()).await?;

    // connect to db
    println!("Connecting to mysql database... ");
    let mut mysql = MysqlManager::connect(profile)?;

    // create channels
    let ctrl_c_events = ctrl_channel()?;
    let (sender, receiver) = bounded::<String>(100);

    // start Fluvio consumer thread
    let consumer = fluvio::consumer(&profile.topic(), 0).await?;
    let offset = Offset::absolute(offset_store.offset()).unwrap();
    async_std::task::spawn(consume(consumer, offset, sender));

    loop {
        select! {
            recv(receiver) -> msg => {
                match msg {
                    Ok(msg) => {
                        mysql.update_database(msg)?;
                        offset_store.increment_offset().await?;
                    }
                    Err(err) => {
                        println!("{}", err.to_string());
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

async fn consume(
    consumer: PartitionConsumer,
    offset: Offset,
    sender: Sender<String>,
) -> Result<(), FluvioError> {
    let mut stream = consumer.stream(offset).await?;

    // read read from producer and print to terminal
    while let Ok(response) = stream.next().await {
        for batch in response.partition.records.batches {
            for record in batch.records {
                if let Some(bytes) = record.value().inner_value() {
                    let msg = String::from_utf8(bytes).expect("error vec => string");
                    sender.send(msg).expect("error sending message");
                }
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
        println!("Error: {}", err.to_string());
    }
}
