use super::super::storage::kv::KvStore;
use super::utils;
use super::worker::{TaskItem, Worker};
use bettermq::{AckReply, AckRequest};
use bettermq::{DataItem, DequeueReply, DequeueRequest};
use bettermq::{EnqueueReply, EnqueueRequest, InnerMsg};
use bettermq::{NackReply, NackRequest};
use prost::Message;
use std::sync::{Arc, RwLock};
use tonic::{Request, Response, Status};
use tracing::info;
pub mod bettermq {
    tonic::include_proto!("bettermq");
}

struct SharedState {
    kv_store: Box<dyn KvStore>,
    worker: Worker,
    seq_no: u64,
}

pub struct PriorityQueueSvc {
    state: Arc<RwLock<SharedState>>,
    node_id: String,
}

pub fn make_one_queue(kv_store: Box<dyn KvStore>, node_id: &String) -> PriorityQueueSvc {
    let mut seq_no: u64 = 0;
    match kv_store.max_key() {
        Ok(max_key_raw) => {
            let mut dst = [0u8; 8];
            dst.clone_from_slice(&max_key_raw[0..8]);
            seq_no = u64::from_be_bytes(dst);
        }
        Err(_) => {}
    }
    let worker = Worker::default();
    let _worker_r = worker.start();
    rebuild_index(&kv_store, &worker);
    let service = PriorityQueueSvc {
        state: Arc::new(RwLock::new(SharedState {
            worker: worker,
            kv_store: kv_store,
            seq_no: seq_no,
        })),
        node_id: node_id.clone(),
    };
    info!("seq_no: {:?}", seq_no);
    service
}

fn rebuild_index(kv_store: &Box<dyn KvStore>, worker: &Worker) {
    let mut start = vec![0 as u8; 1];
    let end = vec![255 as u8; 8];
    let mut total = 0 as u64;
    loop {
        let mut buffer = Vec::<(Vec<u8>, Vec<u8>)>::with_capacity(100);
        let scan_result = kv_store.scan(&start, &end, 100, &mut buffer);
        if buffer.len() == 0 {
            break;
        } else {
            total += buffer.len() as u64;
        }
        info!("rebuild {:} items", total);
        if !scan_result.is_err() {
            for (k, v) in buffer {
                start = k.clone();
                let inner_msg = InnerMsg::decode(v.as_slice()).unwrap();
                let task_item = TaskItem {
                    priority: inner_msg.req.unwrap().priority,
                    timestamp: inner_msg.timestatmp,
                    message_id: k,
                };
                worker.add_task(task_item);
            }
            start.extend(vec![0 as u8; 1])
        } else {
            panic!("rebuild index failed");
        }
    }
}

impl PriorityQueueSvc {
    pub fn enqueue(
        &self,
        request: Request<EnqueueRequest>,
    ) -> Result<Response<EnqueueReply>, Status> {
        info!("{:?}", request);
        let mut state = self.state.write().unwrap();
        state.seq_no += 1;
        let cur_seq = state.seq_no;
        let message_id = cur_seq.to_be_bytes().to_vec();
        let now = utils::timestamp();
        let task_item = TaskItem {
            priority: request.get_ref().priority,
            timestamp: now + request.get_ref().deliver_after as u64 * 1000,
            message_id: message_id.clone(),
        };
        let mut value_buf = Vec::<u8>::with_capacity(100);
        let inner_msg = InnerMsg {
            req: Some(request.get_ref().clone()),
            timestatmp: task_item.timestamp,
        };
        let _r = inner_msg.encode(&mut value_buf);
        match state.kv_store.set(&message_id, value_buf) {
            Ok(_) => {}
            Err(err) => {
                return Err(Status::unknown(err.to_string().clone()));
            }
        }
        state.worker.add_task(task_item);
        let reply = EnqueueReply {
            message_id: format!("{:}_{:}", self.node_id, cur_seq),
        };
        Ok(Response::new(reply))
    }

    pub fn dequeue(
        &self,
        request: Request<DequeueRequest>,
    ) -> Result<Response<DequeueReply>, Status> {
        info!("{:?}", request);
        let task_items: Vec<TaskItem>;
        {
            let state = self.state.write().unwrap();
            task_items = state.worker.fetch_tasks(request.get_ref().count as u32);
        }
        let state = self.state.read().unwrap();
        let reply_items = task_items
            .iter()
            .filter_map(|ti| {
                let value_buf = state.kv_store.get(&ti.message_id);
                match value_buf {
                    Ok(value_buf) => {
                        let s_message_id = utils::msgid_to_str(&ti.message_id);
                        let req = InnerMsg::decode(value_buf.as_slice()).unwrap().req.unwrap();
                        Some(DataItem {
                            message_id: s_message_id,
                            payload: req.payload,
                            meta: req.meta,
                        })
                    }
                    Err(_) => None,
                }
            })
            .collect();
        if request.get_ref().lease_duration > 0 {
            let retry_after = request.get_ref().lease_duration;
            for task in &task_items {
                let retry_task = task.delayed_copy(retry_after);
                state.worker.add_task(retry_task);
            }
        }
        let reply = DequeueReply { items: reply_items };
        Ok(Response::new(reply))
    }

    pub fn ack(&self, request: Request<AckRequest>) -> Result<Response<AckReply>, Status> {
        info!("{:?}", request);
        let message_id = utils::msgid_to_raw(&request.get_ref().message_id);
        let mut state = self.state.write().unwrap();
        state.worker.cancel_task(&message_id);
        {
            match state.kv_store.remove(&message_id) {
                Ok(_) => {}
                Err(err) => {
                    return Err(Status::unknown(err.to_string().clone()));
                }
            }
        }
        let reply = AckReply {};
        Ok(Response::new(reply))
    }

    pub fn nack(&self, request: Request<NackRequest>) -> Result<Response<NackReply>, Status> {
        info!("{:?}", request);
        let reply = NackReply {};
        Ok(Response::new(reply))
    }
}
