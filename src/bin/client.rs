use crate::bettermq::priority_queue_client::PriorityQueueClient;
use crate::bettermq::{
    AckRequest, CreateTopicRequest, DequeueRequest, EnqueueRequest, GetActiveTopicsRequest,
    NackRequest, RemoveTopicRequest,
};
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
                        .value_name("DELIVERY AFTER(ms)"),
                )
                .arg(
                    Arg::with_name("priority")
                        .short("r")
                        .long("priority")
                        .default_value("0")
                        .value_name("PRIORITY"),
                )
                .arg(
                    Arg::with_name("benchmark")
                        .short("b")
                        .long("benchmark")
                        .default_value("10")
                        .value_name("FOR MANY TIMES"),
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
                        .value_name("LEASE(ms)"),
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
        .subcommand(
            SubCommand::with_name("stats")
                .about("get statistics of server")
                .arg(
                    Arg::with_name("host")
                        .short("h")
                        .long("host")
                        .default_value("http://127.0.0.1:8404")
                        .value_name("HOST ADDRESS"),
                ),
        )
        .subcommand(
            SubCommand::with_name("create")
                .about("create a topic")
                .arg(
                    Arg::with_name("topic")
                        .short("t")
                        .long("topic")
                        .default_value("")
                        .value_name("TOPIC"),
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
            SubCommand::with_name("remove")
                .about("remove a topic")
                .arg(
                    Arg::with_name("topic")
                        .short("t")
                        .long("topic")
                        .default_value("")
                        .value_name("TOPIC"),
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
            SubCommand::with_name("nack")
                .about("nack a message")
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
                )
                .arg(
                    Arg::with_name("meta")
                        .short("m")
                        .long("meta")
                        .default_value("")
                        .value_name("NEW MEATA INFO"),
                )
                .arg(
                    Arg::with_name("after")
                        .short("a")
                        .long("after")
                        .default_value("0")
                        .value_name("DELIVER AFTER (ms)"),
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
        ("nack", Some(subm)) => {
            run_nack(subm).await?;
        }
        ("stats", Some(subm)) => {
            run_stats(subm).await?;
        }
        ("create", Some(subm)) => {
            run_create(subm).await?;
        }
        ("remove", Some(subm)) => {
            run_remove(subm).await?;
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
        payload = fs::read(file_name)?;
    }
    let mut n = 1;
    if opts.occurrences_of("benchmark") > 0 {
        n = opts.value_of("benchmark").unwrap().parse::<i32>().unwrap();
    }
    for _i in 0..n {
        let request = tonic::Request::new(EnqueueRequest {
            topic: opts.value_of("topic").unwrap().into(),
            payload: payload.clone(),
            meta: opts.value_of("meta").unwrap().into(),
            priority: opts.value_of("priority").unwrap().parse::<i32>().unwrap(),
            deliver_after: opts.value_of("after").unwrap().parse::<u32>().unwrap(),
        });
        let response = client.enqueue(request).await?;
        println!("{:?}", response);
    }
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

async fn run_nack(opts: &ArgMatches<'_>) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = make_conn(opts).await?;
    let message_id = opts.value_of("id").unwrap().into();
    let request = tonic::Request::new(NackRequest {
        message_id: message_id,
        topic: opts.value_of("topic").unwrap().into(),
        meta: opts.value_of("meta").unwrap().into(),
        deliver_after: opts.value_of("after").unwrap().parse::<u32>().unwrap(),
    });
    let response = client.nack(request).await?;
    println!("{:?}", response);
    Ok(())
}

async fn run_stats(opts: &ArgMatches<'_>) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = make_conn(opts).await?;
    let request = tonic::Request::new(GetActiveTopicsRequest {});
    let response = client.get_active_topics(request).await?;
    for st in &response.get_ref().topics {
        println!("{:?}", st);
    }
    Ok(())
}

async fn run_create(opts: &ArgMatches<'_>) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = make_conn(opts).await?;
    let request = tonic::Request::new(CreateTopicRequest {
        topic: opts.value_of("topic").unwrap().into(),
    });
    let response = client.create_topic(request).await?;
    println!("{:?}", response);
    Ok(())
}

async fn run_remove(opts: &ArgMatches<'_>) -> Result<(), Box<dyn std::error::Error>> {
    let mut client = make_conn(opts).await?;
    let request = tonic::Request::new(RemoveTopicRequest {
        topic: opts.value_of("topic").unwrap().into(),
    });
    let response = client.remove_topic(request).await?;
    println!("{:?}", response);
    Ok(())
}
