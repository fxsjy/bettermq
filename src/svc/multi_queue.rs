use tracing::info;
use tonic::{Request,Response,Status};
use super::priority_queue::bettermq;
use super::priority_queue::make_one_queue;
use bettermq::{EnqueueRequest, EnqueueReply};
use bettermq::{DequeueRequest, DequeueReply};
use bettermq::{AckRequest, AckReply};
use bettermq::{NackRequest, NackReply};
use bettermq::{GetActiveTopicsRequest, GetActiveTopicsReply};
use bettermq::priority_queue_server::PriorityQueue;
use super::priority_queue::PriorityQueueSvc;
use super::super::storage::kv;
use super::super::storage::kv::DbKind;
use std::sync::{Arc,RwLock};
use std::collections::HashMap;

#[derive(Default)]
pub struct MultiQueueSvc {
	topics_svc: Arc<RwLock<HashMap<String, PriorityQueueSvc>>>,
}

#[tonic::async_trait]
impl PriorityQueue for MultiQueueSvc {
	async fn enqueue(&self,
		request: Request<EnqueueRequest>,
	) -> Result<Response<EnqueueReply>, Status> {
		let topic_name = request.get_ref().topic.clone();
		let topics_svc = self.topics_svc.read().unwrap();
		let svc = topics_svc.get(&topic_name);
		match svc {
			Some(svc) => {
				svc.enqueue(request)
			},
			None => {
				Err(Status::not_found(topic_name))
			}
		}
	}

	async fn dequeue(&self,
		request: Request<DequeueRequest>,
	) -> Result<Response<DequeueReply>, Status> {
		let topic_name = request.get_ref().topic.clone();
		let topics_svc = self.topics_svc.read().unwrap();
		let svc = topics_svc.get(&topic_name);
		match svc {
			Some(svc) => {
				svc.dequeue(request)
			},
			None => {
				Err(Status::not_found(topic_name))
			}
		}
	}

	async fn ack(&self,
		request: Request<AckRequest>,
	) -> Result<Response<AckReply>, Status> {
		let topic_name = request.get_ref().topic.clone();
		let topics_svc = self.topics_svc.read().unwrap();
		let svc = topics_svc.get(&topic_name);
		match svc {
			Some(svc) => {
				svc.ack(request)
			},
			None => {
				Err(Status::not_found(topic_name))
			}
		}
	}

	async fn nack(&self,
		request: Request<NackRequest>,
	) -> Result<Response<NackReply>, Status> {
		let topic_name = request.get_ref().topic.clone();
		let topics_svc = self.topics_svc.read().unwrap();
		let svc = topics_svc.get(&topic_name);
		match svc {
			Some(svc) => {
				svc.nack(request)
			},
			None => {
				Err(Status::not_found(topic_name))
			}
		}
	}

	async fn get_active_topics(&self,
		request: Request<GetActiveTopicsRequest>,
	) -> Result<Response<GetActiveTopicsReply>, Status> {
		info!("{:?}", request);
		let reply = GetActiveTopicsReply {
			topics: vec!["topic-1".into(),"topic-2".into()]
		};
		Ok(Response::new(reply))
	}
}


pub fn new(dir: String, node_id: String, topics: Vec<String>) -> bettermq::priority_queue_server::PriorityQueueServer<MultiQueueSvc> {
	let multi_queue = MultiQueueSvc::default();
	{
		let mut topic_svcs = multi_queue.topics_svc.write().unwrap();
		for topic_name in topics{
			let sub_dir = format!("{:}/{:}",dir, topic_name);
			let kv_store = kv::new_kvstore(DbKind::SLED, sub_dir).unwrap();
			let service = make_one_queue(kv_store, &node_id);
			topic_svcs.insert(topic_name, service);
		}
	}
	bettermq::priority_queue_server::PriorityQueueServer::new(multi_queue)
}

