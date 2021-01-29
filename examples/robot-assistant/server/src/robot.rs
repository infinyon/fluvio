use serde::{Serialize, Deserialize};
use anyhow::Result;

pub struct Robot {
    state_id: usize,
    states: Vec<State>,
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

pub enum Message {
    Text(String),
    Number(usize),
}

impl Robot {
    pub fn new() -> Result<Self> {
        let state_id = 0;
        let yaml = include_str!("../robot.yaml");
        let states: Vec<State> = serde_yaml::from_str(&yaml)?;
        Ok(Robot { state_id, states })
    }

    pub fn state(&self) -> State {
        self.states[self.state_id].clone()
    }

    pub fn process(&mut self, message: Message) -> State {
        let cur = self.states[self.state_id].clone();
        let next_id = match (cur, message) {
            (State::Text { next: next_id, .. }, Message::Text(_)) => next_id,
            (State::Number { items, .. }, Message::Number(next_id)) => {
                if items.iter().any(|item| item.next == next_id) {
                    next_id
                } else {
                    self.state_id
                }
            }
            (_, _) => self.state_id,
        };
        self.state_id = next_id;
        self.states[self.state_id].clone()
    }
}

#[test]
fn robot() -> Result<()> {
    let mut robot: Robot = Robot::new()?;
    robot.process(Message::Text("hello".to_string()));
    assert_eq!(robot.state_id, 1);
    robot.process(Message::Number(2));
    assert_eq!(robot.state_id, 2);
    robot.process(Message::Text("hello".to_string()));
    assert_eq!(robot.state_id, 10);
    robot.process(Message::Number(1));
    assert_eq!(robot.state_id, 1);
    robot.process(Message::Number(11));
    assert_eq!(robot.state_id, 1);
    robot.process(Message::Number(3));
    assert_eq!(robot.state_id, 3);
    robot.process(Message::Number(4));
    assert_eq!(robot.state_id, 3);
    robot.process(Message::Text("hello".to_string()));
    assert_eq!(robot.state_id, 10);
    robot.process(Message::Number(11));
    assert_eq!(robot.state_id, 11);
    robot.process(Message::Text("hello".to_string()));
    assert_eq!(robot.state_id, 11);
    Ok(())
}
