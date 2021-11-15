use super::utils;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::collections::BinaryHeap;
use std::collections::HashSet;
use std::collections::LinkedList;
use std::ops::Bound::{Excluded, Included};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::{task, time};
use tracing::info;

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd, Default)]
pub struct TaskItem {
    pub priority: i32,
    pub timestamp: u64,
    pub message_id: Vec<u8>,
}

impl TaskItem {
    pub fn delayed_copy(&self, seconds: i32) -> TaskItem {
        TaskItem {
            priority: self.priority,
            message_id: self.message_id.clone(),
            timestamp: self.timestamp + (seconds * 1000) as u64,
        }
    }
}

#[derive(Default)]
pub struct Worker {
    tasks: Arc<Mutex<TodoTasks>>,
}

#[derive(Default)]
struct TodoTasks {
    ready_queue: BinaryHeap<Reverse<TaskItem>>,
    time_wheel: BTreeMap<u64, LinkedList<TaskItem>>,
    in_wheel: HashSet<Vec<u8>>,
}

impl Worker {
    pub fn start(&self) -> Result<(), Box<dyn std::error::Error>> {
        let tasks = self.tasks.clone();
        let forever = task::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(100));
            loop {
                interval.tick().await;
                let now = utils::timestamp();
                let mut tasks = tasks.lock().unwrap();
                let window = (Excluded(0), Included(now));
                let todo_list = tasks.time_wheel.range(window);
                let mut near_slots = Vec::<u64>::with_capacity(10);
                for (slot, _) in todo_list {
                    near_slots.push(slot.clone());
                }
                for slot in near_slots {
                    let task_items = tasks.time_wheel.remove(&slot).unwrap();
                    for item in task_items {
                        if tasks.in_wheel.contains(&item.message_id) {
                            {
                                tasks.in_wheel.remove(&item.message_id);
                            }
                            tasks.ready_queue.push(Reverse(item));
                        }
                    }
                }
            }
        });
        //let _r = forever.await;
        info!("worker start, {:?}", forever);
        Ok(())
    }

    pub fn add_task(&self, item: TaskItem) -> () {
        let now = utils::timestamp();
        let mut tasks = self.tasks.lock().unwrap();
        if item.timestamp <= now {
            tasks.ready_queue.push(Reverse(item));
        } else {
            {
                tasks.in_wheel.insert(item.message_id.clone());
            }
            let ls = tasks
                .time_wheel
                .entry(item.timestamp)
                .or_insert(LinkedList::<TaskItem>::new());
            ls.push_back(item);
        }
    }

    pub fn fetch_tasks(&self, count: u32) -> Vec<TaskItem> {
        let mut tasks = self.tasks.lock().unwrap();
        let mut items = Vec::<TaskItem>::with_capacity(100);
        for _i in 0..count {
            match tasks.ready_queue.pop() {
                Some(Reverse(top)) => {
                    items.push(top);
                }
                None => break,
            }
        }
        items
    }

    pub fn cancel_task(&self, message_id: &Vec<u8>) -> bool {
        let mut tasks = self.tasks.lock().unwrap();
        if !tasks.in_wheel.contains(message_id) {
            false
        } else {
            tasks.in_wheel.remove(message_id)
        }
    }
}
