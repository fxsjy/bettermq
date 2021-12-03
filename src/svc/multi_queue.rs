use crate::storage::kv;
use crate::storage::kv::DbKind;
use crate::svc::priority_queue::bettermq;
use crate::svc::priority_queue::make_one_queue;
use crate::svc::priority_queue::PriorityQueueSvc;
use bettermq::priority_queue_server::PriorityQueue;
use bettermq::{AckReply, AckRequest};
use bettermq::{
    CreateTopicReply, CreateTopicRequest, GetActiveTopicsReply, GetActiveTopicsRequest,
};
use bettermq::{DequeueReply, DequeueRequest};
use bettermq::{EnqueueReply, EnqueueRequest};
use bettermq::{NackReply, NackRequest};
use std::collections::HashMap;
use std::fs;
use std::sync::{Arc, RwLock};
use tonic::{Request, Response, Status};

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
        _request: Request<GetActiveTopicsRequest>,
    ) -> Result<Response<GetActiveTopicsReply>, Status> {
        let topics_svc = self.topics_svc.read().unwrap();
        let topics_stats = topics_svc
            .iter()
            .map(|(_topic_name, topic_svc)| topic_svc.get_stats())
            .collect();
        let reply = GetActiveTopicsReply {
            topics: topics_stats,
        };
        Ok(Response::new(reply))
    }

    async fn create_topic(
        &self,
        _request: Request<CreateTopicRequest>,
    ) -> Result<Response<CreateTopicReply>, Status> {
        let topics_svc = self.topics_svc.write().unwrap();
        let reply = CreateTopicReply {};
        Ok(Response::new(reply))
    }
}

fn list_topics_from_dir(dir: &String) -> Vec<String> {
    let mut topics = Vec::<String>::new();
    for full_path in fs::read_dir(dir).unwrap() {
        let short_name: String = full_path.unwrap().file_name().to_str().unwrap().into();
        if short_name.ends_with("_index") {
            continue;
        }
        topics.push(short_name);
    }
    topics
}

pub fn new(
    dir: String,
    node_id: String,
    config_topics: Vec<String>,
) -> bettermq::priority_queue_server::PriorityQueueServer<MultiQueueSvc> {
    let multi_queue = MultiQueueSvc::default();
    {
        let mut topic_svcs = multi_queue.topics_svc.write().unwrap();
        let mut all_topics: Vec<String> = config_topics.into_iter().collect();
        let topics_listed = list_topics_from_dir(&dir);
        for topic in topics_listed {
            if !all_topics.contains(&topic) {
                all_topics.push(topic);
            }
        }
        for topic_name in all_topics {
            let sub_dir = format!("{:}/{:}", dir, topic_name);
            let index_dir = format!("{:}_index", sub_dir);
            let msg_store = kv::new_kvstore(DbKind::SLED, sub_dir).unwrap();
            let index_store = kv::new_kvstore(DbKind::SLED, index_dir).unwrap();
            let service = make_one_queue(msg_store, index_store, &node_id, &topic_name);
            topic_svcs.insert(topic_name, service);
        }
    }
    bettermq::priority_queue_server::PriorityQueueServer::new(multi_queue)
}
