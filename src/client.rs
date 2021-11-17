use bettermq::priority_queue_client::PriorityQueueClient;
use bettermq::{AckRequest, DequeueRequest, EnqueueRequest};
use clap::{App, Arg, ArgMatches, SubCommand};
use std::fs;

pub mod bettermq {
    tonic::include_proto!("bettermq");
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = App::new("bmq-cli")
        .subcommand(
            SubCommand::with_name("enqueue")
                .about("put a new message into queue")
                .arg(
                    Arg::with_name("topic")
                        .short("t")
                        .long("topic")
                        .default_value("root")
                        .value_name("TOPIC"),
                )
                .arg(
                    Arg::with_name("meta")
                        .short("m")
                        .long("meta")
                        .default_value("meta")
                        .value_name("METAINFO"),
                )
                .arg(
                    Arg::with_name("payload")
                        .short("p")
                        .long("payload")
                        .default_value("")
                        .value_name("MESSAGE DATA"),
                )
                .arg(
                    Arg::with_name("file")
                        .short("f")
                        .long("file")
                        .value_name("FILE NAME FOR PAYLOAD"),
                )
                .arg(
                    Arg::with_name("after")
                        .short("a")
                        .long("after")
                        .default_value("0")
                        .value_name("DELIVERY AFTER"),
                )
                .arg(
                    Arg::with_name("priority")
                        .short("r")
                        .long("priority")
                        .default_value("0")
                        .value_name("PRIORITY"),
                )
                .arg(
                    Arg::with_name("host")
                        .short("h")
                        .long("host")
                        .default_value("http://127.0.0.1:8404")
                        .value_name("HOST ADDRESS"),
                ),
        )
        .subcommand(
            SubCommand::with_name("dequeue")
                .about("get messages from queue")
                .arg(
                    Arg::with_name("topic")
                        .short("t")
                        .long("topic")
                        .default_value("root")
                        .value_name("TOPIC"),
                )
                .arg(
                    Arg::with_name("count")
                        .short("c")
                        .long("count")
                        .default_value("1")
                        .value_name("COUNT"),
                )
                .arg(
                    Arg::with_name("lease")
                        .short("l")
                        .long("lease")
                        .default_value("0")
                        .value_name("LEASE"),
                )
                .arg(
                    Arg::with_name("host")
                        .short("h")
                        .long("host")
                        .default_value("http://127.0.0.1:8404")
                        .value_name("HOST ADDRESS"),
                ),
        )
        .subcommand(
            SubCommand::with_name("ack")
                .about("ack a message")
                .arg(
                    Arg::with_name("topic")
                        .short("t")
                        .long("topic")
                        .default_value("root")
                        .value_name("TOPIC"),
                )
                .arg(
                    Arg::with_name("id")
                        .short("i")
                        .long("id")
                        .required(true)
                        .value_name("MESSAGE ID"),
                )
                .arg(
                    Arg::with_name("host")
                        .short("h")
                        .long("host")
                        .default_value("http://127.0.0.1:8404")
                        .value_name("HOST ADDRESS"),
                ),
        )
        .get_matches();
    match opts.subcommand() {
        ("enqueue", Some(subm)) => {
            run_enqueue(subm).await?;
        }
        ("dequeue", Some(subm)) => {
            run_dequeue(subm).await?;
        }
        ("ack", Some(subm)) => {
            run_ack(subm).await?;
        }
        _ => {
            return Ok(());
        }
    };
    Ok(())
}

async fn run_enqueue(opts: &ArgMatches<'_>) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = make_conn(opts).await?;
    let mut payload = opts.value_of("payload").unwrap().as_bytes().to_vec();
    if opts.occurrences_of("file") > 0 {
        let file_name = opts.value_of("file").unwrap();
        payload = fs::read_to_string(file_name)?.as_bytes().to_vec();
    }
    let request = tonic::Request::new(EnqueueRequest {
        topic: opts.value_of("topic").unwrap().into(),
        payload: payload,
        meta: opts.value_of("meta").unwrap().into(),
        priority: opts.value_of("priority").unwrap().parse::<i32>().unwrap(),
        deliver_after: opts.value_of("after").unwrap().parse::<i32>().unwrap(),
    });
    let response = client.enqueue(request).await?;
    println!("{:?}", response);
    Ok(())
}

async fn make_conn(
    opts: &ArgMatches<'_>,
) -> Result<PriorityQueueClient<tonic::transport::Channel>, Box<dyn std::error::Error>> {
    let host = String::from(opts.value_of("host").unwrap());
    let channel = tonic::transport::Channel::from_shared(host)
        .unwrap()
        .connect()
        .await?;
    let client = PriorityQueueClient::new(channel);
    Ok(client)
}

async fn run_dequeue(opts: &ArgMatches<'_>) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = make_conn(opts).await?;
    let lease = opts.value_of("lease").unwrap().parse::<i32>().unwrap();
    let request = tonic::Request::new(DequeueRequest {
        topic: opts.value_of("topic").unwrap().into(),
        count: opts.value_of("count").unwrap().parse::<i32>().unwrap(),
        lease_duration: lease,
    });
    let response = client.dequeue(request).await?;
    for item in response.get_ref().items.iter() {
        println!("{:?}", item);
    }
    Ok(())
}

async fn run_ack(opts: &ArgMatches<'_>) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = make_conn(opts).await?;
    let message_id = opts.value_of("id").unwrap().into();
    let request = tonic::Request::new(AckRequest {
        message_id: message_id,
        topic: opts.value_of("topic").unwrap().into(),
    });
    let response = client.ack(request).await?;
    println!("{:?}", response);
    Ok(())
}
