use actix::prelude::*;
use actix_session::{CookieSession, Session};
use actix_web::{web, get, App, HttpServer, Error, HttpRequest, HttpResponse, Result};
use actix_web_actors::ws;
use actix_files::NamedFile;
use async_std::task::spawn;
use fluvio::{producer, TopicProducer, Offset};
use futures::StreamExt;
use serde::{Serialize, Deserialize};
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::Mutex;
use uuid::Uuid;

const TOPIC_ID: &str = "robot-assistant";

#[derive(Message, Debug, Clone, Serialize, Deserialize)]
#[rtype(result = "()")]
pub enum Command {
    Text(String),
    Number(usize),
}

impl Command {
    pub fn new(text: String) -> Self {
        if let Ok(num) = text.parse::<usize>() {
            Command::Number(num)
        } else {
            Command::Text(text)
        }
    }
}

#[derive(Serialize, Deserialize)]
pub struct Message {
    pub session_id: Uuid,
    pub cmd: Command,
}

impl Message {
    pub fn new(session_id: Uuid, cmd: Command) -> Self {
        Message { session_id, cmd }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum State {
    Text { prompt: String, next: usize },
    Number { prompt: String, items: Vec<Item> },
    End { message: String },
}

impl State {
    pub fn start(&self) -> String {
        if let State::Text { prompt, .. } = self {
            prompt.to_string()
        } else {
            "start".to_string()
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Item {
    pub answer: String,
    pub next: usize,
}

pub struct RobotClient {
    uuid: Uuid,
    state_id: usize,
    states: Vec<State>,
    shared_producer: Arc<Mutex<TopicProducer>>,
}

impl RobotClient {
    fn new(uuid: Uuid, shared_producer: Arc<Mutex<TopicProducer>>) -> Self {
        let state_id = 0;
        let yaml = include_str!("../robot.yaml");
        let states: Vec<State> = serde_yaml::from_str(&yaml).expect("yaml");
        RobotClient {
            uuid,
            state_id,
            states,
            shared_producer,
        }
    }

    fn send_to_fluvio(
        &mut self,
        ctx: &mut <Self as Actor>::Context,
        text: String,
        shared_producer: Arc<Mutex<TopicProducer>>,
    ) {
        let uuid = self.uuid;
        async move {
            let cmd = Command::new(text);
            let message = Message::new(uuid, cmd);
            let json = serde_json::to_string(&message).expect("json");
            let guard = shared_producer.lock().await;
            guard.send_record(&json, 0).await.expect("sent");
        }
        .into_actor(self)
        .wait(ctx);
    }

    fn send_current_state(&mut self, ctx: &mut <Self as Actor>::Context) {
        let state = self.state();
        match state {
            State::Text { prompt, .. } => {
                ctx.text(format!("<div class=\"bot-message\">{}</div>", prompt))
            }
            State::Number { prompt, items } => {
                ctx.text(format!("<div class=\"bot-message\">{}</div>", prompt));
                for item in items {
                    ctx.text(format!("<div>[ {} ] {}</div>", item.next, item.answer));
                }
            }
            State::End { message } => ctx.text(format!("<div>{}</div>", message)),
        }
    }

    fn state(&self) -> State {
        self.states[self.state_id].clone()
    }
}

impl Actor for RobotClient {
    type Context = ws::WebsocketContext<Self>;

    fn started(&mut self, ctx: &mut Self::Context) {
        self.send_current_state(ctx);
    }
}

impl Handler<Command> for RobotClient {
    type Result = ();

    fn handle(&mut self, cmd: Command, ctx: &mut Self::Context) {
        let cur = self.states[self.state_id].clone();
        let cmd_text = match &cmd {
            Command::Text(text) => text.to_string(),
            Command::Number(num) => num.to_string(),
        };
        ctx.text(format!(
            "<div style=\"text-align: right;\">{}</div>",
            cmd_text
        ));
        let next_id = match (cur, cmd) {
            (State::Text { next: next_id, .. }, Command::Text(_)) => next_id,
            (State::Number { items, .. }, Command::Number(next_id)) => {
                if items.iter().any(|item| item.next == next_id) {
                    next_id
                } else {
                    self.state_id
                }
            }
            (_, _) => self.state_id,
        };
        self.state_id = next_id;
        self.send_current_state(ctx);
    }
}

impl StreamHandler<Result<ws::Message, ws::ProtocolError>> for RobotClient {
    fn handle(&mut self, msg: Result<ws::Message, ws::ProtocolError>, ctx: &mut Self::Context) {
        match msg {
            Ok(ws::Message::Ping(_)) => {}
            Ok(ws::Message::Pong(_)) => {}
            Ok(ws::Message::Text(text)) => {
                self.send_to_fluvio(ctx, text, self.shared_producer.clone())
            }
            Ok(ws::Message::Binary(_)) => {}
            Ok(ws::Message::Close(reason)) => {
                ctx.close(reason);
                ctx.stop();
            }
            _ => ctx.stop(),
        }
    }
}

#[get("/pkg/{filename:.*}")]
async fn client_files(req: HttpRequest) -> Result<NamedFile> {
    let filename = req.match_info().query("filename");
    let path: PathBuf = ["examples", "robot-assistant", "pkg", filename]
        .iter()
        .collect();
    Ok(NamedFile::open(path)?)
}

#[get("/{filename:.*}")]
async fn html_files(req: HttpRequest) -> Result<NamedFile> {
    let filename = req.match_info().query("filename");
    let path: PathBuf = ["examples", "robot-assistant", "html", filename]
        .iter()
        .collect();
    Ok(NamedFile::open(path)?)
}

async fn ws_index(
    r: HttpRequest,
    stream: web::Payload,
    session: Session,
    shared_producer: web::Data<Arc<Mutex<TopicProducer>>>,
) -> Result<HttpResponse, Error> {
    let uuid = if let Some(uuid) = session.get::<Uuid>("uuid")? {
        uuid
    } else {
        Uuid::new_v4()
    };
    session.set("uuid", uuid)?;
    let robot_client = RobotClient::new(uuid, shared_producer.get_ref().clone());
    match ws::start_with_addr(robot_client, &r, stream) {
        Ok((addr, res)) => {
            spawn(async move {
                let consumer = fluvio::consumer(TOPIC_ID, 0).await.expect("consumer");
                let mut stream = consumer
                    .stream(Offset::beginning())
                    .await
                    .expect("consumer");
                while let Some(Ok(record)) = stream.next().await {
                    if let Some(bytes) = record.try_into_bytes() {
                        let json: String = String::from_utf8_lossy(&bytes).to_string();
                        let message: Message = serde_json::from_str(&json).expect("message");
                        let Message { session_id, cmd } = message;
                        if session_id == uuid {
                            addr.do_send(cmd)
                        }
                    }
                }
            });
            Ok(res)
        }
        Err(err) => Err(err),
    }
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let producer: TopicProducer = producer(TOPIC_ID).await.expect("producer");
    let shared_producer = Arc::new(Mutex::new(producer));
    HttpServer::new(move || {
        App::new()
            .data(shared_producer.clone())
            .wrap(CookieSession::signed(&[0; 32]).secure(false))
            .service(web::resource("/ws/").to(ws_index))
            .service(client_files)
            .service(html_files)
    })
    .bind("127.0.0.1:8080")?
    .run()
    .await
}
