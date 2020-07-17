use crate::TestOption;
use super::message::*;

pub async fn produce_message(option: &TestOption) {
    if option.produce.produce_iteration == 1 {
        cli::produce_message_with_cli(option).await;
    } else {
        produce_message_with_api(option).await;
    }
}

pub async fn produce_message_with_api(option: &TestOption) {
    use flv_client::ClusterConfig;
   

    let config = ClusterConfig::lookup_profile(None).expect("connect");
    let mut cluster = config.connect().await.expect("should connect");
    let mut producer = cluster.producer(&option.topic_name,0).await.expect("producer");

    for i in 0..option.produce.produce_iteration {
        let message = generate_message(i, option);

        producer.send_record(message).await.expect("message sent");

        println!("message sent: {}", i);
    }
}

mod cli {

    use std::io::Write;
    use std::process::Stdio;

    use utils::bin::get_fluvio;
    use crate::cli::TestOption;
    use crate::util::CommandUtil;

    use super::*;

    pub async fn produce_message_with_cli(option: &TestOption) {
        println!("starting produce");

        let produce_count = option.produce.produce_iteration;
        for i in 0..produce_count {
            produce_message(i, &option.topic_name, option);
            //sleep(Duration::from_millis(10)).await
        }
    }

    fn produce_message(_index: u16, topic_name: &str, option: &TestOption) {
        use std::io;

        let mut child = get_fluvio()
            .expect("no fluvio")
            .log(option.log.as_ref())
            .stdin(Stdio::piped())
            .arg("produce")
            .arg(topic_name)
            .print()
            .spawn()
            .expect("no child");

        let stdin = child.stdin.as_mut().expect("Failed to open stdin");
        let msg = generate_message(0, option);
        stdin
            .write_all(msg.as_slice())
            .expect("Failed to write to stdin");

        let output = child.wait_with_output().expect("Failed to read stdout");
        io::stdout().write_all(&output.stdout).unwrap();
        io::stderr().write_all(&output.stderr).unwrap();
        assert!(output.status.success());

        println!("send message of len {}", msg.len());
    }
}
