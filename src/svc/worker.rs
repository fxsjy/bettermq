use crate::svc::utils;
use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::collections::BinaryHeap;
use std::collections::HashSet;
use std::collections::LinkedList;
use std::ops::Bound::{Excluded, Included};
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Duration;
use tokio::sync::Notify;
use tokio::{task, time};
use tracing::info;

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd, Default)]
pub struct TaskItem {
    pub priority: i32,
    pub timestamp: u64,
    pub message_id: Vec<u8>,
}

pub struct QueueStats {
    pub ready_size: u64,
    pub delayed_size: u64,
}

impl TaskItem {
    pub fn delayed_copy(&self, milli_seconds: i32) -> TaskItem {
        let now = utils::timestamp();
        TaskItem {
            priority: self.priority,
            message_id: self.message_id.clone(),
            timestamp: now + milli_seconds as u64,
        }
    }
}

#[derive(Default)]
pub struct Worker {
    tasks: Arc<Mutex<TodoTasks>>,
    tk_handles: Vec<tokio::task::JoinHandle<()>>,
    notifier: Arc<Notify>,
}

#[derive(Default)]
struct TodoTasks {
    ready_queue: BinaryHeap<Reverse<TaskItem>>,
    time_wheel: BTreeMap<u64, LinkedList<TaskItem>>,
    in_wheel: HashSet<Vec<u8>>,
    in_ready: HashSet<Vec<u8>>,
    stop_flag: bool,
}

impl Worker {
    pub fn start(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        let tasks = self.tasks.clone();
        self.notifier = Arc::new(Notify::new());
        let notifier = self.notifier.clone();
        let handler = task::spawn(async move {
            let mut interval = time::interval(Duration::from_millis(5));
            info!("worker start");
            loop {
                interval.tick().await;
                let now = utils::timestamp();
                let mut tasks = tasks.lock().unwrap();
                if tasks.stop_flag {
                    break;
                }
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
                                tasks.in_ready.insert(item.message_id.clone());
                            }
                            tasks.ready_queue.push(Reverse(item));
                        }
                    }
                }
            }
            info!("worker stopped");
            notifier.notify_one();
        });
        self.tk_handles.push(handler);
        Ok(())
    }

    pub fn add_task(&self, item: TaskItem) -> () {
        let now = utils::timestamp();
        let mut tasks = self.tasks.lock().unwrap();
        if item.timestamp <= now {
            if tasks.in_ready.contains(&item.message_id) {
                return;
            }
            tasks.in_ready.insert(item.message_id.clone());
            tasks.ready_queue.push(Reverse(item));
        } else {
            if tasks.in_wheel.contains(&item.message_id) {
                return;
            }
            tasks.in_ready.remove(&item.message_id);
            tasks.in_wheel.insert(item.message_id.clone());
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
        let mut ct = 0;
        while ct < count {
            match tasks.ready_queue.pop() {
                Some(Reverse(top)) => {
                    if !tasks.in_ready.contains(&top.message_id) {
                        continue;
                    }
                    tasks.in_ready.remove(&top.message_id);
                    items.push(top);
                    ct += 1;
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

    pub fn stats(&self) -> QueueStats {
        let tasks = self.tasks.lock().unwrap();
        QueueStats {
            ready_size: tasks.ready_queue.len() as u64,
            delayed_size: tasks.in_wheel.len() as u64,
        }
    }

    pub async fn stop(&self) -> bool {
        {
            let mut tasks = self.tasks.lock().unwrap();
            tasks.stop_flag = true;
        }
        self.notifier.notified().await;
        return true;
    }
}
