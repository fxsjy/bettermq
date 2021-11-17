use crate::storage::kv::KvStore;
use crate::svc::utils;
use crate::svc::worker::{TaskItem, Worker};
use bettermq::{AckReply, AckRequest};
use bettermq::{DataItem, DequeueReply, DequeueRequest};
use bettermq::{EnqueueReply, EnqueueRequest, InnerIndex};
use bettermq::{NackReply, NackRequest};
use prost::Message;
use std::sync::{Arc, RwLock};
use tonic::{Request, Response, Status};
use tracing::{info, trace};
pub mod bettermq {
    tonic::include_proto!("bettermq");
}

struct SharedState {
    msg_store: Box<dyn KvStore>,
    index_store: Box<dyn KvStore>,
    worker: Worker,
    seq_no: u64,
}

pub struct PriorityQueueSvc {
    state: Arc<RwLock<SharedState>>,
    node_id: String,
}

pub fn make_one_queue(
    msg_store: Box<dyn KvStore>,
    index_store: Box<dyn KvStore>,
    node_id: &String,
) -> PriorityQueueSvc {
    let mut seq_no: u64 = 0;
    match msg_store.max_key() {
        Ok(max_key_raw) => {
            let mut dst = [0u8; 8];
            dst.clone_from_slice(&max_key_raw[0..8]);
            seq_no = u64::from_be_bytes(dst);
        }
        Err(_) => {}
    }
    let worker = Worker::default();
    let _worker_r = worker.start();
    rebuild_index(&index_store, &worker);
    let service = PriorityQueueSvc {
        state: Arc::new(RwLock::new(SharedState {
            worker: worker,
            msg_store: msg_store,
            index_store: index_store,
            seq_no: seq_no,
        })),
        node_id: node_id.clone(),
    };
    info!("seq_no: {:?}", seq_no);
    service
}

fn rebuild_index(index_store: &Box<dyn KvStore>, worker: &Worker) {
    let mut start = vec![0 as u8; 1];
    let end = vec![255 as u8; 8];
    let mut total = 0 as u64;
    loop {
        let mut buffer = Vec::<(Vec<u8>, Vec<u8>)>::with_capacity(100);
        let scan_result = index_store.scan(&start, &end, 100, &mut buffer);
        if buffer.len() == 0 {
            break;
        } else {
            total += buffer.len() as u64;
        }
        info!("rebuild {:} items", total);
        if !scan_result.is_err() {
            for (k, v) in buffer {
                start = k.clone();
                let inner_index = InnerIndex::decode(v.as_slice()).unwrap();
                let task_item = TaskItem {
                    priority: inner_index.priority,
                    timestamp: inner_index.timestamp,
                    message_id: inner_index.message_id,
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
        trace!("{:?}", request);
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
        let mut value_buf = Vec::<u8>::with_capacity(200);
        let _r = request.get_ref().encode(&mut value_buf);
        match state.msg_store.set(&message_id, value_buf) {
            Ok(_) => {}
            Err(err) => {
                return Err(Status::unknown(err.to_string().clone()));
            }
        }
        let mut index_buf = Vec::<u8>::with_capacity(100);
        let inner_index = InnerIndex {
            priority: task_item.priority,
            timestamp: task_item.timestamp,
            message_id: message_id.clone(),
        };
        let _r = inner_index.encode(&mut index_buf);
        match state.index_store.set(&message_id, index_buf) {
            Ok(_) => {}
            Err(err) => {
                return Err(Status::unknown(err.to_string().clone()));
            }
        }
        state.worker.add_task(task_item);
        let reply = EnqueueReply {
            message_id: format!("{:}", cur_seq),
            node_id: self.node_id.clone(),
        };
        Ok(Response::new(reply))
    }

    pub fn dequeue(
        &self,
        request: Request<DequeueRequest>,
    ) -> Result<Response<DequeueReply>, Status> {
        trace!("{:?}", request);
        let task_items: Vec<TaskItem>;
        {
            let state = self.state.write().unwrap();
            task_items = state.worker.fetch_tasks(request.get_ref().count as u32);
            if request.get_ref().lease_duration > 0 {
                let retry_after = request.get_ref().lease_duration;
                for task in &task_items {
                    let retry_task = task.delayed_copy(retry_after);
                    state.worker.add_task(retry_task);
                }
            }
        }
        let reply_items = self.fill_payload(task_items);
        if request.get_ref().lease_duration <= 0 {
            let mut state = self.state.write().unwrap();
            for item in &reply_items {
                self.remove_msg(&mut state, utils::msgid_to_raw(&item.message_id));
            }
        }
        let reply = DequeueReply { items: reply_items };
        Ok(Response::new(reply))
    }

    fn fill_payload(&self, task_items: Vec<TaskItem>) -> Vec<DataItem> {
        let state = self.state.read().unwrap();
        let reply_items: Vec<DataItem> = task_items
            .iter()
            .filter_map(|ti| {
                let value_buf = state.msg_store.get(&ti.message_id);
                match value_buf {
                    Ok(value_buf) => {
                        let s_message_id = utils::msgid_to_str(&ti.message_id);
                        let req = EnqueueRequest::decode(value_buf.as_slice()).unwrap();
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
        reply_items
    }

    pub fn ack(&self, request: Request<AckRequest>) -> Result<Response<AckReply>, Status> {
        trace!("{:?}", request);
        let message_id = utils::msgid_to_raw(&request.get_ref().message_id);
        let mut state = self.state.write().unwrap();
        if !state.worker.cancel_task(&message_id) {
            return Err(Status::not_found("no lease found"));
        }
        if let Some(value) = self.remove_msg(&mut state, message_id) {
            return value;
        }
        let reply = AckReply {};
        Ok(Response::new(reply))
    }

    pub fn nack(&self, request: Request<NackRequest>) -> Result<Response<NackReply>, Status> {
        trace!("{:?}", request);
        let reply = NackReply {};
        Ok(Response::new(reply))
    }

    fn remove_msg(
        &self,
        state: &mut std::sync::RwLockWriteGuard<SharedState>,
        message_id: Vec<u8>,
    ) -> Option<Result<Response<AckReply>, Status>> {
        match state.index_store.remove(&message_id) {
            Ok(_) => {}
            Err(err) => {
                return Some(Err(Status::unknown(err.to_string().clone())));
            }
        }
        let msgid = utils::msgid_to_u64(&message_id);
        if msgid == state.seq_no {
            return None;
        }
        match state.msg_store.remove(&message_id) {
            Ok(_) => {}
            Err(err) => {
                return Some(Err(Status::unknown(err.to_string().clone())));
            }
        }
        None
    }
}
