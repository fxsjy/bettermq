mod storage;
mod svc;
use clap::{app_from_crate, crate_authors, crate_description, crate_name, crate_version};
use serde_derive::Deserialize;
use std::fs;
use tokio::time;
use tokio::time::Duration;
use tonic::transport::Server;
use tracing::info;
use tracing_subscriber;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let opts = app_from_crate!()
        .arg(
            clap::Arg::with_name("config")
                .short("c")
                .long("config")
                .help("Configuration file path")
                .takes_value(true)
                .default_value("./config/bettermq.yaml"),
        )
        .get_matches();
    let cfg = Config::new(opts.value_of("config").unwrap())?;
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();
    let addr = cfg.listen_grpc.parse()?;
    let root_dir = cfg.data_dir.clone();
    let svr = svc::multi_queue::new(cfg.data_dir, cfg.node_id, cfg.topics);
    tokio::task::spawn(async move { clean_garbage(&root_dir).await });
    info!("happy start");
    Server::builder().add_service(svr).serve(addr).await?;
    Ok(())
}

#[derive(Debug, Deserialize)]
struct Config {
    node_id: String,
    listen_grpc: String,
    data_dir: String,
    log_level: String,
    topics: Vec<String>,
}

impl Config {
    fn new(file: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let mut c = config::Config::new();
        c.set_default("node_id", "1")?;
        c.set_default("listen_grpc", "127.0.0.1:8402")?;
        c.set_default("data_dir", "/tmp/demo_queue")?;
        c.set_default("log_level", "info")?;
        c.set_default("topics", vec!["root"])?;
        c.merge(config::File::with_name(file))?;
        c.merge(config::Environment::with_prefix("BETTERMQ"))?;
        Ok(c.try_into()?)
    }
}

async fn clean_garbage(dir: &String) {
    let mut interval = time::interval(Duration::from_millis(5000));
    loop {
        interval.tick().await;
        for full_path in fs::read_dir(dir).unwrap() {
            let short_name: String = full_path.unwrap().file_name().to_str().unwrap().into();
            if short_name.ends_with("_gc") {
                let data_dir = format!("{}/{}", dir, short_name);
                let index_dir: String = format!("{}/{}", dir, short_name.replace("_gc", "_index"));
                info!("remove {},{}", data_dir, index_dir);
                let _r1 = fs::remove_dir_all(data_dir);
                let _r2 = fs::remove_dir_all(index_dir);
            }
        }
    }
}
