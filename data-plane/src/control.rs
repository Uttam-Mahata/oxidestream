tonic::include_proto!("control");

use std::sync::Arc;
use std::sync::atomic::{AtomicI32, Ordering};
use std::collections::HashMap;
use tokio::sync::RwLock;
use tonic::{Request, Response, Status};
use crate::control::worker_control_server::WorkerControl;
use crate::control::control_plane_client::ControlPlaneClient;
use std::path::PathBuf;

pub struct WorkerState {
    pub worker_id: String,
    pub host: String,
    pub control_port: i32,
    pub flight_port: i32,
    pub master_address: String,
    pub data_dir: PathBuf,
    pub num_cores: i32,
    pub total_memory_mb: i64,
    pub active_tasks: AtomicI32,
    pub running_tasks: Arc<RwLock<HashMap<String, tokio::sync::oneshot::Sender<()>>>>,
}

pub struct WorkerControlImpl {
    pub state: Arc<WorkerState>,
}

#[tonic::async_trait]
impl WorkerControl for WorkerControlImpl {
    async fn submit_task(
        &self,
        request: Request<SubmitTaskRequest>,
    ) -> Result<Response<SubmitTaskResponse>, Status> {
        let task = request.into_inner();
        let task_id = task.task_id.clone();
        let stage_id = task.stage_id.clone();
        
        println!("Received SubmitTaskRequest: task_id={}, stage_id={}, stage_type={:?}", 
                 task_id, stage_id, task.stage_type);

        let (cancel_tx, mut cancel_rx) = tokio::sync::oneshot::channel();
        {
            let mut tasks = self.state.running_tasks.write().await;
            tasks.insert(task_id.clone(), cancel_tx);
        }

        self.state.active_tasks.fetch_add(1, Ordering::SeqCst);

        let master_address = self.state.master_address.clone();
        let state_clone = self.state.clone();
        let task_id_clone = task_id.clone();
        
        tokio::spawn(async move {
            let control_client = match ControlPlaneClient::connect(master_address.clone()).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Failed to connect to Master at {}: {}", master_address, e);
                    state_clone.active_tasks.fetch_sub(1, Ordering::SeqCst);
                    let mut tasks = state_clone.running_tasks.write().await;
                    tasks.remove(&task_id_clone);
                    return;
                }
            };

            let flight_address = format!("{}:{}", state_clone.host, state_clone.flight_port);
            let mut control_client_cancel = control_client.clone();

            tokio::select! {
                _ = &mut cancel_rx => {
                    println!("Task {} was cancelled", task_id_clone);
                    let update_req = TaskStatusUpdateRequest {
                        task_id: task_id_clone.clone(),
                        stage_id: stage_id.clone(),
                        worker_id: state_clone.worker_id.clone(),
                        status: TaskStatus::Failed as i32,
                        error_message: "Task cancelled".to_string(),
                        produced_partitions: Vec::new(),
                        table_stats: None,
                    };
                    let _ = control_client_cancel.update_task_status(update_req).await;
                }
                _ = async {
                    if task.stage_type == StageType::Map as i32 {
                        crate::executor::execute_map_task(
                            task.task_id,
                            task.stage_id,
                            task.input_files,
                            task.map_sql,
                            task.num_partitions,
                            task.partition_key_columns,
                            task.broadcast_files,
                            task.broadcast_table_names,
                            state_clone.worker_id.clone(),
                            flight_address,
                            state_clone.data_dir.clone(),
                            control_client,
                        ).await;
                    } else if task.stage_type == StageType::Reduce as i32 {
                        crate::executor::execute_reduce_task(
                            task.task_id,
                            task.stage_id,
                            task.reduce_sql,
                            task.output_partition_id,
                            task.shuffle_inputs,
                            task.coalesced_partitions,
                            task.broadcast_files,
                            task.broadcast_table_names,
                            task.output_dir,
                            state_clone.worker_id.clone(),
                            control_client,
                        ).await;
                    }
                } => {
                    println!("Finished task {}", task_id_clone);
                }
            }

            state_clone.active_tasks.fetch_sub(1, Ordering::SeqCst);
            let mut tasks = state_clone.running_tasks.write().await;
            tasks.remove(&task_id_clone);
        });

        Ok(Response::new(SubmitTaskResponse {
            success: true,
            message: format!("Task {} submitted successfully", task_id),
        }))
    }

    async fn plan_query(
        &self,
        request: Request<PlanQueryRequest>,
    ) -> Result<Response<PlanQueryResponse>, Status> {
        let req = request.into_inner();
        println!("Received PlanQueryRequest: sql={}", req.sql);

        match crate::planner::plan_query(&req.sql, &req.input_files, req.num_partitions).await {
            Ok(plan) => Ok(Response::new(PlanQueryResponse {
                success: true,
                message: "ok".to_string(),
                map_sql: plan.map_sql,
                reduce_sql: plan.reduce_sql,
                partition_key_columns: plan.partition_key_columns,
            })),
            Err(e) => Ok(Response::new(PlanQueryResponse {
                success: false,
                message: format!("{}", e),
                map_sql: String::new(),
                reduce_sql: String::new(),
                partition_key_columns: Vec::new(),
            })),
        }
    }

    async fn cancel_task(
        &self,
        request: Request<CancelTaskRequest>,
    ) -> Result<Response<CancelTaskResponse>, Status> {
        let req = request.into_inner();
        let task_id = req.task_id;
        
        let mut tasks = self.state.running_tasks.write().await;
        if let Some(cancel_tx) = tasks.remove(&task_id) {
            let _ = cancel_tx.send(());
            Ok(Response::new(CancelTaskResponse {
                success: true,
                message: format!("Task {} cancelled", task_id),
            }))
        } else {
            Ok(Response::new(CancelTaskResponse {
                success: false,
                message: format!("Task {} not found or already finished", task_id),
            }))
        }
    }
}

pub async fn register_worker(
    state: &WorkerState,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut addr = state.master_address.clone();
    if !addr.starts_with("http://") && !addr.starts_with("https://") {
        addr = format!("http://{}", addr);
    }
    
    let mut client = ControlPlaneClient::connect(addr).await?;
    let req = RegisterWorkerRequest {
        worker_id: state.worker_id.clone(),
        host: state.host.clone(),
        control_port: state.control_port,
        flight_port: state.flight_port,
        num_cores: state.num_cores,
        total_memory_mb: state.total_memory_mb,
    };
    
    let response = client.register_worker(req).await?;
    if response.into_inner().success {
        println!("Successfully registered worker {} with master", state.worker_id);
        Ok(())
    } else {
        Err("Registration rejected by master".into())
    }
}

pub async fn start_heartbeat_loop(
    state: Arc<WorkerState>,
) {
    let master_address = state.master_address.clone();
    
    tokio::spawn(async move {
        let mut addr = master_address;
        if !addr.starts_with("http://") && !addr.starts_with("https://") {
            addr = format!("http://{}", addr);
        }

        let mut interval = tokio::time::interval(tokio::time::Duration::from_secs(2));
        let mut system = sysinfo::System::new_all();

        loop {
            interval.tick().await;

            let mut client = match ControlPlaneClient::connect(addr.clone()).await {
                Ok(c) => c,
                Err(e) => {
                    eprintln!("Heartbeat failed to connect to master: {}", e);
                    continue;
                }
            };

            system.refresh_cpu_usage();
            system.refresh_memory();
            let cpu_usage_pct = system.global_cpu_info().cpu_usage();
            let memory_used_mb = (system.used_memory() / 1024 / 1024) as i64;
            let active_tasks = state.active_tasks.load(Ordering::SeqCst);

            let req = HeartbeatRequest {
                worker_id: state.worker_id.clone(),
                cpu_usage_pct,
                memory_used_mb,
                active_tasks,
            };

            match client.heartbeat(req).await {
                Ok(_) => {
                    // Heartbeat sent successfully
                }
                Err(e) => {
                    eprintln!("Failed to send heartbeat: {}", e);
                }
            }
        }
    });
}
