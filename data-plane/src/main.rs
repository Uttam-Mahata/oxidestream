use clap::Parser;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::AtomicI32;
use std::collections::HashMap;
use std::time::Instant;
use tokio::sync::RwLock;

pub mod control;
pub mod executor;
pub mod flight;
pub mod streaming;
pub mod ml_graph;
pub mod connectors;
pub mod codegen;
pub mod planner;
pub mod http_server;

#[derive(Parser, Debug)]
#[command(name = "data-plane", about = "OxideStream Data Plane Worker Node")]
struct Args {
    #[arg(long, default_value = "")]
    worker_id: String,

    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    #[arg(long, default_value = "50052")]
    control_port: i32,

    #[arg(long, default_value = "50053")]
    flight_port: i32,

    #[arg(long, default_value = "http://127.0.0.1:50051")]
    master_address: String,

    #[arg(long, default_value = "./shuffle_data")]
    data_dir: String,

    #[arg(long, default_value = "4")]
    num_cores: i32,

    #[arg(long, default_value = "8192")]
    total_memory_mb: i64,

    #[arg(long, default_value = "executor")]
    mode: String,

    #[arg(long, default_value = "9090")]
    http_port: u16,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args = Args::parse();

    let worker_id = if args.worker_id.is_empty() {
        uuid::Uuid::new_v4().to_string()
    } else {
        args.worker_id
    };

    let data_dir = PathBuf::from(&args.data_dir);
    tokio::fs::create_dir_all(&data_dir).await?;

    let state = Arc::new(control::WorkerState {
        worker_id,
        host: args.host.clone(),
        control_port: args.control_port,
        flight_port: args.flight_port,
        master_address: args.master_address.clone(),
        data_dir,
        num_cores: args.num_cores,
        total_memory_mb: args.total_memory_mb,
        active_tasks: AtomicI32::new(0),
        running_tasks: Arc::new(RwLock::new(HashMap::new())),
    });

    let start_time = Instant::now();

    let http_state = state.clone();
    let http_port = args.http_port;
    tokio::spawn(async move {
        if let Err(e) = http_server::start_http_server(http_port, http_state, start_time).await {
            eprintln!("HTTP server error: {}", e);
        }
    });

    let is_shuffle = args.mode == "shuffle-service";

    // 1. Register with Master (only if not shuffle-service)
    if !is_shuffle {
        println!("Registering worker with master at {}...", state.master_address);
        if let Err(e) = control::register_worker(&state).await {
            eprintln!("Warning: Failed to register worker with master: {}. Will retry/continue...", e);
        }

        // 2. Start heartbeat loop
        println!("Starting heartbeat loop...");
        control::start_heartbeat_loop(state.clone()).await;
    }

    if is_shuffle {
        // 3. Start Arrow Flight Server
        let flight_addr: SocketAddr = format!("0.0.0.0:{}", args.flight_port).parse()?;
        println!("Starting Arrow Flight server on {}...", flight_addr);
        let flight_server = flight::ShuffleFlightServer {
            data_dir: state.data_dir.clone(),
        };
        let flight_service = arrow_flight::flight_service_server::FlightServiceServer::new(flight_server);
        let flight_handle = tokio::spawn(async move {
            if let Err(e) = tonic::transport::Server::builder()
                .add_service(flight_service)
                .serve(flight_addr)
                .await
            {
                eprintln!("Arrow Flight server error: {}", e);
            }
        });
        let _ = flight_handle.await;
    } else {
        // 4. Start WorkerControl gRPC Server
        let control_addr: SocketAddr = format!("0.0.0.0:{}", args.control_port).parse()?;
        println!("Starting WorkerControl server on {}...", control_addr);
        let control_server = control::WorkerControlImpl {
            state: state.clone(),
        };
        let control_service = control::worker_control_server::WorkerControlServer::new(control_server);
        let control_handle = tokio::spawn(async move {
            if let Err(e) = tonic::transport::Server::builder()
                .add_service(control_service)
                .serve(control_addr)
                .await
            {
                eprintln!("WorkerControl server error: {}", e);
            }
        });
        let _ = control_handle.await;
    }

    Ok(())
}
