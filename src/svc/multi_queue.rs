use crate::storage::kv;
use crate::storage::kv::DbKind;
use crate::svc::priority_queue::bettermq;
use crate::svc::priority_queue::make_one_queue;
use crate::svc::priority_queue::PriorityQueueSvc;
use bettermq::priority_queue_server::PriorityQueue;
use bettermq::{AckReply, AckRequest};
use bettermq::{DequeueReply, DequeueRequest};
use bettermq::{EnqueueReply, EnqueueRequest};
use bettermq::{GetActiveTopicsReply, GetActiveTopicsRequest};
use bettermq::{NackReply, NackRequest};
use std::collections::HashMap;
use std::sync::{Arc, RwLock};
use tonic::{Request, Response, Status};
use tracing::info;

#[derive(Default)]
pub struct MultiQueueSvc {
    topics_svc: Arc<RwLock<HashMap<String, PriorityQueueSvc>>>,
}

#[tonic::async_trait]
impl PriorityQueue for MultiQueueSvc {
    async fn enqueue(
        &self,
        request: Request<EnqueueRequest>,
    ) -> Result<Response<EnqueueReply>, Status> {
        let topic_name = request.get_ref().topic.clone();
        let topics_svc = self.topics_svc.read().unwrap();
        let svc = topics_svc.get(&topic_name);
        match svc {
            Some(svc) => svc.enqueue(request),
            None => Err(Status::not_found(topic_name)),
        }
    }

    async fn dequeue(
        &self,
        request: Request<DequeueRequest>,
    ) -> Result<Response<DequeueReply>, Status> {
        let topic_name = request.get_ref().topic.clone();
        let topics_svc = self.topics_svc.read().unwrap();
        let svc = topics_svc.get(&topic_name);
        match svc {
            Some(svc) => svc.dequeue(request),
            None => Err(Status::not_found(topic_name)),
        }
    }

    async fn ack(&self, request: Request<AckRequest>) -> Result<Response<AckReply>, Status> {
        let topic_name = request.get_ref().topic.clone();
        let topics_svc = self.topics_svc.read().unwrap();
        let svc = topics_svc.get(&topic_name);
        match svc {
            Some(svc) => svc.ack(request),
            None => Err(Status::not_found(topic_name)),
        }
    }

    async fn nack(&self, request: Request<NackRequest>) -> Result<Response<NackReply>, Status> {
        let topic_name = request.get_ref().topic.clone();
        let topics_svc = self.topics_svc.read().unwrap();
        let svc = topics_svc.get(&topic_name);
        match svc {
            Some(svc) => svc.nack(request),
            None => Err(Status::not_found(topic_name)),
        }
    }

    async fn get_active_topics(
        &self,
        request: Request<GetActiveTopicsRequest>,
    ) -> Result<Response<GetActiveTopicsReply>, Status> {
        info!("{:?}", request);
        let reply = GetActiveTopicsReply {
            topics: vec!["topic-1".into(), "topic-2".into()],
        };
        Ok(Response::new(reply))
    }
}

pub fn new(
    dir: String,
    node_id: String,
    topics: Vec<String>,
) -> bettermq::priority_queue_server::PriorityQueueServer<MultiQueueSvc> {
    let multi_queue = MultiQueueSvc::default();
    {
        let mut topic_svcs = multi_queue.topics_svc.write().unwrap();
        for topic_name in topics {
            let sub_dir = format!("{:}/{:}", dir, topic_name);
            let index_dir = format!("{:}_index", sub_dir);
            let msg_store = kv::new_kvstore(DbKind::SLED, sub_dir).unwrap();
            let index_store = kv::new_kvstore(DbKind::SLED, index_dir).unwrap();
            let service = make_one_queue(msg_store, index_store, &node_id);
            topic_svcs.insert(topic_name, service);
        }
    }
    bettermq::priority_queue_server::PriorityQueueServer::new(multi_queue)
}
