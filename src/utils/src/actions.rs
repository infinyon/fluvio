//!
//! # Actions Template
//!
//! Template to enable chaining actions in a list.
//! Operations supported:
//! * push, count compare, traverse(iter.)
//!
use std::collections::vec_deque::Iter;
use std::collections::vec_deque::IntoIter;
use std::collections::VecDeque;


/// queue of Action
#[derive(Debug,Clone)]
pub struct Actions<T>(VecDeque<T>);


impl<T> ::std::default::Default for Actions<T> {
    fn default() -> Self {
        Self(VecDeque::new())
    }
}


impl<T> ::std::cmp::PartialEq for Actions<T>
where
    T: PartialEq,
{
    fn eq(&self, other: &Actions<T>) -> bool {
        let local_actions = &self.0;
        let other_actions = &other.0;

        if local_actions.len() != other_actions.len() {
            return false;
        }
        for (idx, action) in local_actions.iter().enumerate() {
            if action != &other_actions[idx] {
                return false;
            }
        }

        true
    }
}

impl<T> Actions<T> {

    pub fn add_all(&mut self,items: Vec<T>) {
        for item in items.into_iter() {
            self.push(item);
        }
    }

    pub fn push(&mut self, action: T) {
        self.0.push_back(action);
    }

    pub fn push_once(&mut self, action: T)
    where
        T: PartialEq,
    {
        let mut found = false;
        for local_action in self.0.iter() {
            if local_action == &action {
                found = true;
                break;
            }
        }
        if !found {
            self.0.push_back(action);
        }
    }

    pub fn pop_front(&mut self) -> Option<T> {
        self.0.pop_front()
    }


    pub fn count(&self) -> usize {
        self.0.len()
    }

    pub fn iter<'a>(&'a self) -> Iter<'a, T> {
        self.0.iter()
    }

    pub fn into_iter(self) -> IntoIter<T> {
        self.0.into_iter()
    }
}


impl <T>From<Vec<T>> for Actions<T> {
    fn from(vec: Vec<T>) -> Self {
        let mut actions = Self::default();
        for item in vec.into_iter() {
            actions.push(item);
        }
        actions
    }
}