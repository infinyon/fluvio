mod robot;

use tide_websockets::{Message, WebSocket};
use async_std::prelude::*;
use async_std::task::spawn;
use futures::future::join;
use anyhow::Result;
use std::io::{Error, ErrorKind};
use tide::sessions::SessionMiddleware;
use tide::sessions::CookieStore;
use uuid::Uuid;
use std::process::Command;
use robot::Robot;
use robot::State;

#[async_std::main]
async fn main() -> Result<()> {
    println!("server");
    tide::log::start();
    let mut app = tide::new();
    app.with(SessionMiddleware::new(
        CookieStore::new(),
        b"936DA01F9ABD4d9d80C702AF85C822A8",
    ));

    app.with(tide::utils::Before(
        |mut request: tide::Request<()>| async move {
            let session = request.session_mut();
            let topic_id = if let Some(topic_id) = session.get("topic_id") {
                topic_id
            } else {
                let my_uuid = Uuid::new_v4();
                let topic_id = format!("{}", my_uuid.to_simple());
                // FIXME
                Command::new("cargo")
                    .args(&[
                        "run",
                        "--bin",
                        "fluvio",
                        "topic",
                        "create",
                        topic_id.as_str(),
                    ])
                    .output()
                    .expect("Failed to execute command");
                topic_id
            };
            session.insert("topic_id", topic_id).unwrap();
            request
        },
    ));
    app.at("/").serve_dir("examples/robot-assistant/html")?;
    app.at("/pkg/").serve_dir("examples/robot-assistant/pkg/")?;
    app.at("/ws/")
        .get(WebSocket::new(|req, ws_stream| async move {
            let topic_id: String = req.session().get("topic_id").unwrap();
            let topic_id_clone = topic_id.clone();
            let mut ws_stream_clone = ws_stream.clone();
            let mut robot = Robot::new().unwrap();
            ws_stream
                .send_string(format!("<div>{}</div>", robot.state().start()))
                .await
                .map_err(|_| Error::new(ErrorKind::Other, "oh no!"))?;

            let produce_handle = spawn(async move {
                let producer = fluvio::producer(topic_id_clone).await?;
                while let Some(Ok(Message::Text(input))) = ws_stream_clone.next().await {
                    println!("{}", input);
                    producer.send_record(&input, 0).await?;
                }
                Ok(())
            });
            let topic_id_clone = topic_id.clone();
            let consume_handle = spawn(async move {
                let consumer = fluvio::consumer(topic_id_clone, 0).await?;
                let mut fluvio_stream = consumer.stream(fluvio::Offset::beginning()).await?;
                while let Some(Ok(record)) = fluvio_stream.next().await {
                    let bytes = record.as_ref();
                    let s = String::from_utf8_lossy(&bytes);
                    ws_stream
                        .send_string(format!("<div style=\"text-align: right;\">{}</div>", s))
                        .await
                        .map_err(|_| Error::new(ErrorKind::Other, "oh no!"))?;
                    let message = if let Ok(num) = s.parse::<usize>() {
                        robot::Message::Number(num)
                    } else {
                        robot::Message::Text(s.to_string())
                    };
                    let state = robot.process(message);
                    match state {
                        State::Text { prompt, .. } => ws_stream
                            .send_string(format!("<div class=\"bot-message\">{}</div>", prompt))
                            .await
                            .map_err(|_| Error::new(ErrorKind::Other, "oh no!"))?,
                        State::Number { prompt, items } => {
                            ws_stream
                                .send_string(format!("<div class=\"bot-message\">{}</div>", prompt))
                                .await
                                .map_err(|_| Error::new(ErrorKind::Other, "oh no!"))?;
                            for item in items {
                                ws_stream
                                    .send_string(format!(
                                        "<div>[ {} ] {}</div>",
                                        item.next, item.answer
                                    ))
                                    .await
                                    .map_err(|_| Error::new(ErrorKind::Other, "oh no!"))?;
                            }
                        }
                        State::End { message } => ws_stream
                            .send_string(format!("<div>{}</div>", message))
                            .await
                            .map_err(|_| Error::new(ErrorKind::Other, "oh no!"))?,
                    }
                }
                Ok(())
            });
            let (_, _): (Result<()>, Result<()>) = join(produce_handle, consume_handle).await;
            Ok(())
        }));

    app.listen("127.0.0.1:8080").await?;
    Ok(())
}
