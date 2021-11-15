use bettermq::priority_queue_client::PriorityQueueClient;
use bettermq::{AckRequest, DequeueRequest, EnqueueRequest};

pub mod bettermq {
    tonic::include_proto!("bettermq");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut client = PriorityQueueClient::connect("http://127.0.0.1:8404").await?;
    let request = tonic::Request::new(EnqueueRequest {
        topic: "root".into(),
        payload: String::from("xxxxxxx").as_bytes().to_vec(),
        meta: String::from("helloworld"),
        priority: 12,
        deliver_after: 10,
    });

    let response = client.enqueue(request).await?;
    println!("enqueue RESPONSE={:?}", response);

    let request = tonic::Request::new(DequeueRequest {
        topic: "root".into(),
        count: 100,
        lease_duration: 10,
    });

    let response = client.dequeue(request).await?;
    for item in response.get_ref().items.iter() {
        println!("dequeue RESPONSE={:?}", item);
        let request = tonic::Request::new(AckRequest {
            message_id: item.message_id.clone(),
            topic: "root".into(),
        });
        let response = client.ack(request).await?;
        println!("ACK RESPONSE={:?}", response);
    }
    Ok(())
}
