use axum::{extract::State, http::StatusCode, routing::get, Json, Router};
use serde::Serialize;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Instant;

use crate::control::WorkerState;

#[derive(Clone)]
pub struct HttpState {
    pub worker_state: Arc<WorkerState>,
    pub start_time: Instant,
}

#[derive(Serialize)]
struct HealthResponse {
    status: String,
    worker_id: String,
    uptime_seconds: u64,
}

#[derive(Serialize)]
struct MetricsResponse {
    worker_id: String,
    host: String,
    num_cores: i32,
    total_memory_mb: i64,
    active_tasks: i32,
    cpu_usage_pct: f32,
    memory_used_mb: i64,
    uptime_seconds: u64,
}

#[derive(Serialize)]
struct TaskInfo {
    task_id: String,
}

#[derive(Serialize)]
struct TasksResponse {
    tasks: Vec<TaskInfo>,
    count: usize,
}

async fn health_check(State(state): State<HttpState>) -> (StatusCode, Json<HealthResponse>) {
    let uptime = state.start_time.elapsed().as_secs();
    (
        StatusCode::OK,
        Json(HealthResponse {
            status: "ok".to_string(),
            worker_id: state.worker_state.worker_id.clone(),
            uptime_seconds: uptime,
        }),
    )
}

async fn metrics(State(state): State<HttpState>) -> (StatusCode, Json<MetricsResponse>) {
    let mut system = sysinfo::System::new_all();
    system.refresh_cpu_usage();
    system.refresh_memory();

    let cpu_usage_pct = system.global_cpu_info().cpu_usage();
    let memory_used_mb = (system.used_memory() / 1024 / 1024) as i64;
    let uptime = state.start_time.elapsed().as_secs();

    (
        StatusCode::OK,
        Json(MetricsResponse {
            worker_id: state.worker_state.worker_id.clone(),
            host: state.worker_state.host.clone(),
            num_cores: state.worker_state.num_cores,
            total_memory_mb: state.worker_state.total_memory_mb,
            active_tasks: state.worker_state.active_tasks.load(Ordering::SeqCst),
            cpu_usage_pct,
            memory_used_mb,
            uptime_seconds: uptime,
        }),
    )
}

async fn tasks(State(state): State<HttpState>) -> (StatusCode, Json<TasksResponse>) {
    let running = state.worker_state.running_tasks.read().await;
    let task_list: Vec<TaskInfo> = running
        .keys()
        .map(|id| TaskInfo {
            task_id: id.clone(),
        })
        .collect();
    let count = task_list.len();

    (
        StatusCode::OK,
        Json(TasksResponse {
            tasks: task_list,
            count,
        }),
    )
}

pub async fn start_http_server(
    port: u16,
    worker_state: Arc<WorkerState>,
    start_time: Instant,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let state = HttpState {
        worker_state,
        start_time,
    };

    let app = Router::new()
        .route("/health", get(health_check))
        .route("/metrics", get(metrics))
        .route("/tasks", get(tasks))
        .layer(
            tower_http::trace::TraceLayer::new_for_http(),
        )
        .layer(
            tower_http::cors::CorsLayer::permissive(),
        )
        .with_state(state);

    let addr = format!("0.0.0.0:{}", port);
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!("HTTP server listening on {}", addr);

    axum::serve(listener, app).await?;

    Ok(())
}
