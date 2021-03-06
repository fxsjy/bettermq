use crate::storage::kv::KvStore;
use crate::svc::utils;
use crate::svc::worker::{TaskItem, Worker};
use bettermq::TopicStats;
use bettermq::{AckReply, AckRequest};
use bettermq::{DataItem, DequeueReply, DequeueRequest};
use bettermq::{EnqueueReply, EnqueueRequest, InnerIndex};
use bettermq::{NackReply, NackRequest};
use prost::Message;
use rayon::prelude::*;
use std::sync::{Arc, RwLock};
use tonic::{Request, Response, Status};
use tracing::{info, trace};

pub mod bettermq {
    tonic::include_proto!("bettermq");
}

struct SharedState {
    msg_store: Box<dyn KvStore>,
    index_store: Box<dyn KvStore>,
    seq_no: u64,
}

pub struct PriorityQueueSvc {
    state: Arc<RwLock<SharedState>>,
    node_id: String,
    topic: String,
    worker: Box<Worker>,
}

pub fn make_one_queue(
    msg_store: Box<dyn KvStore>,
    index_store: Box<dyn KvStore>,
    node_id: &String,
    topic: &String,
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
    let mut worker = Worker::default();
    let _worker_r = worker.start();
    rebuild_index(&index_store, &worker);
    let service = PriorityQueueSvc {
        state: Arc::new(RwLock::new(SharedState {
            msg_store: msg_store,
            index_store: index_store,
            seq_no: seq_no,
        })),
        node_id: node_id.clone(),
        topic: topic.clone(),
        worker: Box::new(worker),
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
        let cur_seq: u64;
        {
            let mut state = self.state.write().unwrap();
            state.seq_no += 1;
            cur_seq = state.seq_no;
        }
        let result = self.enqueue_with_id(cur_seq, request);
        match result {
            Ok(value) => Ok(Response::new(value)),
            Err(err) => Err(err),
        }
    }

    fn enqueue_with_id(
        &self,
        cur_seq: u64,
        request: Request<EnqueueRequest>,
    ) -> Result<EnqueueReply, Status> {
        let message_id = cur_seq.to_be_bytes().to_vec();
        let now = utils::timestamp();
        let task_item = TaskItem {
            priority: request.get_ref().priority,
            timestamp: now + request.get_ref().deliver_after as u64,
            message_id: message_id.clone(),
        };
        let mut value_buf = Vec::<u8>::with_capacity(200);
        let _r = request.get_ref().encode(&mut value_buf);
        let mut index_buf = Vec::<u8>::with_capacity(100);
        let inner_index = InnerIndex {
            priority: task_item.priority,
            timestamp: task_item.timestamp,
            message_id: message_id.clone(),
        };
        let _r = inner_index.encode(&mut index_buf);
        {
            let state = self.state.read().unwrap();
            match state.msg_store.set(&message_id, value_buf) {
                Ok(_) => {}
                Err(err) => {
                    return Err(Status::unknown(err.to_string().clone()));
                }
            }
            match state.index_store.set(&message_id, index_buf) {
                Ok(_) => {}
                Err(err) => {
                    return Err(Status::unknown(err.to_string().clone()));
                }
            }
        }
        self.worker.add_task(task_item);
        let reply = EnqueueReply {
            message_id: format!("{:}", cur_seq),
            node_id: self.node_id.clone(),
        };
        Ok(reply)
    }

    pub fn dequeue(
        &self,
        request: Request<DequeueRequest>,
    ) -> Result<Response<DequeueReply>, Status> {
        trace!("{:?}", request);
        let task_items: Vec<TaskItem>;
        task_items = self.worker.fetch_tasks(request.get_ref().count as u32);
        if request.get_ref().lease_duration > 0 {
            let retry_after = request.get_ref().lease_duration;
            for task in &task_items {
                let retry_task = task.delayed_copy(retry_after);
                self.worker.add_task(retry_task);
            }
        }

        let reply_items = self.fill_payload(task_items);
        if request.get_ref().lease_duration <= 0 {
            for item in &reply_items {
                let state = self.state.read().unwrap();
                self.remove_msg(&state, utils::msgid_to_raw(&item.message_id));
            }
        }
        let reply = DequeueReply { items: reply_items };
        Ok(Response::new(reply))
    }

    fn fill_payload(&self, task_items: Vec<TaskItem>) -> Vec<DataItem> {
        let state = self.state.read().unwrap();
        let reply_items: Vec<DataItem> = task_items
            .par_iter()
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
                            priority: req.priority,
                        })
                    }
                    Err(_) => None,
                }
            })
            .collect();
        reply_items
    }

    pub fn get_stats(&self) -> TopicStats {
        let stats = self.worker.stats();
        let stats = TopicStats {
            topic: self.topic.clone(),
            ready_size: stats.ready_size,
            delayed_size: stats.delayed_size,
        };
        stats
    }

    pub fn ack(&self, request: Request<AckRequest>) -> Result<Response<AckReply>, Status> {
        trace!("{:?}", request);
        let message_id = utils::msgid_to_raw(&request.get_ref().message_id);
        if !self.worker.cancel_task(&message_id) {
            return Err(Status::not_found("no lease found"));
        }
        {
            let state = self.state.read().unwrap();
            if let Some(value) = self.remove_msg(&state, message_id) {
                return value;
            }
        }
        let reply = AckReply {};
        Ok(Response::new(reply))
    }

    pub fn nack(&self, request: Request<NackRequest>) -> Result<Response<NackReply>, Status> {
        trace!("{:?}", request);
        let message_id = utils::msgid_to_raw(&request.get_ref().message_id);
        let _canceled = self.worker.cancel_task(&message_id);
        let old_payload: Vec<u8>;
        let old_priority: i32;
        let old_meta: String;
        let new_meta: String;
        {
            let state = self.state.read().unwrap();
            let value_buf = state.msg_store.get(&message_id);
            match value_buf {
                Ok(value_buf) => {
                    let raw_req = EnqueueRequest::decode(value_buf.as_slice()).unwrap();
                    old_payload = raw_req.payload;
                    old_priority = raw_req.priority;
                    old_meta = raw_req.meta;
                }
                Err(err) => {
                    return Err(Status::not_found(err.to_string()));
                }
            }
        }
        if request.get_ref().meta.len() > 0 {
            new_meta = request.get_ref().meta.clone();
        } else {
            new_meta = old_meta;
        }
        let enq_again_request = tonic::Request::new(EnqueueRequest {
            topic: request.get_ref().topic.clone(),
            payload: old_payload,
            meta: new_meta,
            priority: old_priority,
            deliver_after: request.get_ref().deliver_after,
        });
        let seq_no = utils::msgid_to_u64(&message_id);
        let enq_ret = self.enqueue_with_id(seq_no, enq_again_request);
        match enq_ret {
            Ok(_) => {
                let reply = NackReply {};
                return Ok(Response::new(reply));
            }
            Err(err) => {
                return Err(err);
            }
        }
    }

    fn remove_msg(
        &self,
        state: &std::sync::RwLockReadGuard<SharedState>,
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

    pub async fn stop(&self) {
        self.worker.stop().await;
    }
}

#[cfg(test)]
mod tests {
    use super::super::super::storage::kv;
    use super::*;
    use temp_dir::TempDir;
    use tokio::time::{sleep, Duration};
    #[tokio::test]
    async fn basic_put_get() {
        let tmp_dir = TempDir::new().unwrap();
        let sub_dir = tmp_dir.path().to_str().unwrap().into();
        let index_dir = format!("{}_index", sub_dir);
        let msg_store = kv::new_kvstore(kv::DbKind::SLED, sub_dir).unwrap();
        let index_store = kv::new_kvstore(kv::DbKind::SLED, index_dir).unwrap();
        let service = make_one_queue(msg_store, index_store, &"test_node".into(), &"root".into());
        let r1 = tonic::Request::new(EnqueueRequest {
            topic: "test".into(),
            payload: vec![1, 2, 3],
            meta: "r1".into(),
            priority: 1,
            deliver_after: 0,
        });
        let r2 = tonic::Request::new(EnqueueRequest {
            topic: "test".into(),
            payload: vec![1, 2, 3],
            meta: "r2".into(),
            priority: 0,
            deliver_after: 0,
        });
        let r3 = tonic::Request::new(EnqueueRequest {
            topic: "test".into(),
            payload: vec![1, 2, 3],
            meta: "r3".into(),
            priority: 1,
            deliver_after: 0,
        });
        let r4 = tonic::Request::new(EnqueueRequest {
            topic: "test".into(),
            payload: vec![1, 2, 3],
            meta: "r4".into(),
            priority: -1,
            deliver_after: 2,
        });
        let result1 = service.enqueue(r1);
        assert!(result1.is_ok());
        let result2 = service.enqueue(r2);
        assert!(result2.is_ok());
        let result3 = service.enqueue(r3);
        assert!(result3.is_ok());
        let result4 = service.enqueue(r4);
        assert!(result4.is_ok());
        let pops = service
            .dequeue(tonic::Request::new(DequeueRequest {
                topic: "test".into(),
                count: 4,
                lease_duration: 0,
            }))
            .unwrap();
        let mut n = 0 as i32;
        for x in &pops.get_ref().items {
            match n {
                0 => {
                    assert_eq!(x.meta, "r2");
                }
                1 => {
                    assert_eq!(x.meta, "r1");
                }
                2 => {
                    assert_eq!(x.meta, "r3");
                }
                _ => {}
            }
            n = n + 1;
        }
        println!("sleep 2 seconds");
        sleep(Duration::from_millis(2100)).await;
        let pops = service
            .dequeue(tonic::Request::new(DequeueRequest {
                topic: "test".into(),
                count: 1,
                lease_duration: 5,
            }))
            .unwrap();
        assert_eq!(pops.get_ref().items[0].meta, "r4");
        println!("sleep 5 seconds");
        sleep(Duration::from_millis(5100)).await;
        let pops = service
            .dequeue(tonic::Request::new(DequeueRequest {
                topic: "test".into(),
                count: 1,
                lease_duration: 0,
            }))
            .unwrap();
        assert_eq!(pops.get_ref().items[0].meta, "r4");
        println!("sleep 5 seconds");
        sleep(Duration::from_millis(5100)).await;
        let pops = service
            .dequeue(tonic::Request::new(DequeueRequest {
                topic: "test".into(),
                count: 1,
                lease_duration: 0,
            }))
            .unwrap();
        assert_eq!(pops.get_ref().items.len(), 0);
        service.stop().await;
    }
}
