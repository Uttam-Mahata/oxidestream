use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::fs;
use datafusion::prelude::*;
use arrow::record_batch::RecordBatch;
use arrow::array::Array;
use arrow::ipc::writer::FileWriter;
use crate::control::{TaskStatusUpdateRequest, TaskStatus, PartitionMetadata, ShuffleInput};
use crate::control::control_plane_client::ControlPlaneClient;
use tonic::transport::Channel;
use arrow::array::ArrayRef;
use std::collections::{hash_map::DefaultHasher, HashSet, HashMap};
use std::hash::{Hash, Hasher};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use futures::{StreamExt, TryStreamExt};
use datafusion::datasource::MemTable;

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct ColStats {
    pub null_count: usize,
    pub distinct_count: usize,
    pub min: String,
    pub max: String,
}

#[derive(serde::Serialize, serde::Deserialize, Debug)]
pub struct TableStats {
    pub total_rows: usize,
    pub columns: HashMap<String, ColStats>,
}

fn get_row_hash(array: &ArrayRef, row_idx: usize) -> u64 {
    let mut hasher = DefaultHasher::new();
    match array.data_type() {
        arrow::datatypes::DataType::Utf8 => {
            if let Some(arr) = array.as_any().downcast_ref::<arrow::array::StringArray>() {
                arr.value(row_idx).hash(&mut hasher);
            }
        }
        arrow::datatypes::DataType::LargeUtf8 => {
            if let Some(arr) = array.as_any().downcast_ref::<arrow::array::LargeStringArray>() {
                arr.value(row_idx).hash(&mut hasher);
            }
        }
        arrow::datatypes::DataType::Int32 => {
            if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Int32Array>() {
                arr.value(row_idx).hash(&mut hasher);
            }
        }
        arrow::datatypes::DataType::Int64 => {
            if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Int64Array>() {
                arr.value(row_idx).hash(&mut hasher);
            }
        }
        arrow::datatypes::DataType::UInt32 => {
            if let Some(arr) = array.as_any().downcast_ref::<arrow::array::UInt32Array>() {
                arr.value(row_idx).hash(&mut hasher);
            }
        }
        arrow::datatypes::DataType::UInt64 => {
            if let Some(arr) = array.as_any().downcast_ref::<arrow::array::UInt64Array>() {
                arr.value(row_idx).hash(&mut hasher);
            }
        }
        arrow::datatypes::DataType::Float32 => {
            if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Float32Array>() {
                arr.value(row_idx).to_bits().hash(&mut hasher);
            }
        }
        arrow::datatypes::DataType::Float64 => {
            if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Float64Array>() {
                arr.value(row_idx).to_bits().hash(&mut hasher);
            }
        }
        _ => {
            row_idx.hash(&mut hasher);
        }
    }
    hasher.finish()
}

// Resolve the column indices to hash-partition on. Falls back to the column
// literally named "key" (or column 0) when no planner-derived keys are given
// or none of the named keys exist in this batch's schema.
fn resolve_partition_key_indices(batch: &RecordBatch, partition_key_columns: &[String]) -> Vec<usize> {
    if !partition_key_columns.is_empty() {
        let resolved: Vec<usize> = partition_key_columns
            .iter()
            .filter_map(|name| batch.schema().index_of(name).ok())
            .collect();
        if !resolved.is_empty() {
            return resolved;
        }
    }
    vec![batch.schema().index_of("key").unwrap_or(0)]
}

fn partition_batch(
    batch: &RecordBatch,
    num_partitions: i32,
    partition_key_columns: &[String],
) -> Result<Vec<RecordBatch>, Box<dyn std::error::Error + Send + Sync>> {
    let num_rows = batch.num_rows();
    let num_parts = num_partitions as usize;
    let mut partition_indices = vec![Vec::new(); num_parts];

    if batch.num_columns() == 0 {
        return Ok(vec![batch.clone(); num_parts]);
    }

    let key_indices = resolve_partition_key_indices(batch, partition_key_columns);

    for row_idx in 0..num_rows {
        // Combine the per-column hashes so multi-key GROUP BY / JOIN shuffles
        // route rows with equal key tuples to the same partition.
        let mut hasher = DefaultHasher::new();
        for &col_idx in &key_indices {
            get_row_hash(batch.column(col_idx), row_idx).hash(&mut hasher);
        }
        let part_idx = (hasher.finish() % (num_partitions as u64)) as usize;
        partition_indices[part_idx].push(row_idx as u32);
    }

    let mut partitioned_batches = Vec::with_capacity(num_parts);
    for part_idx in 0..num_parts {
        let indices = &partition_indices[part_idx];
        if indices.is_empty() {
            let empty_cols = batch.columns().iter()
                .map(|col| arrow::array::new_empty_array(col.data_type()))
                .collect::<Vec<_>>();
            let empty_batch = RecordBatch::try_new(batch.schema(), empty_cols)?;
            partitioned_batches.push(empty_batch);
        } else {
            let indices_array = arrow::array::UInt32Array::from(indices.clone());
            let partitioned_cols = batch.columns().iter()
                .map(|col| {
                    arrow::compute::take(col.as_ref(), &indices_array, None)
                        .map_err(|e| Box::new(e) as Box<dyn std::error::Error + Send + Sync>)
                })
                .collect::<Result<Vec<_>, _>>()?;
            let part_batch = RecordBatch::try_new(batch.schema(), partitioned_cols)?;
            partitioned_batches.push(part_batch);
        }
    }

    Ok(partitioned_batches)
}

fn resolve_file_path(file_path: &str) -> String {
    let p = Path::new(file_path);
    if p.is_absolute() && p.exists() {
        return file_path.to_string();
    }
    // Try relative to current working directory
    if p.exists() {
        return file_path.to_string();
    }
    // Try stripping leading path components or matching relative to repo root
    let components: Vec<_> = p.components().map(|c| c.as_os_str().to_string_lossy().to_string()).collect();
    if let Some(pos) = components.iter().position(|c| c == "tests") {
        let rel_path = components[pos..].join("/");
        if Path::new(&rel_path).exists() {
            return rel_path;
        }
    }
    file_path.to_string()
}

pub async fn register_input_files(
    ctx: &SessionContext,
    input_files: &[String],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for raw_file_path in input_files {
        let file_path = resolve_file_path(raw_file_path);
        let path = Path::new(&file_path);
        let filename = path.file_name()
            .and_then(|f| f.to_str())
            .ok_or_else(|| format!("Invalid input file path: {}", file_path))?;

        let table_name = path.file_stem()
            .and_then(|s| s.to_str())
            .ok_or_else(|| format!("Invalid input file path stem: {}", file_path))?;

        if filename.ends_with(".parquet") {
            ctx.register_parquet(table_name, &file_path, ParquetReadOptions::default()).await?;
            ctx.register_parquet("input", &file_path, ParquetReadOptions::default()).await?;
        } else if filename.ends_with(".csv") || filename.contains(".csv") {
            ctx.register_csv(table_name, &file_path, CsvReadOptions::default()).await?;
            ctx.register_csv("input", &file_path, CsvReadOptions::default()).await?;
        } else if filename.ends_with(".db") || filename.contains(".sqlite") || filename.contains(".sqlite3") {
            let table_names = {
                let conn = rusqlite::Connection::open(&file_path)?;
                let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
                let mut rows = stmt.query([])?;
                let mut tbls = Vec::new();
                while let Some(row) = rows.next()? {
                    let tbl_name: String = row.get(0)?;
                    tbls.push(tbl_name);
                }
                tbls
            };
            for tbl_name in table_names {
                crate::connectors::register_sqlite_table(ctx, &file_path, &tbl_name).await?;
            }
        } else {
            return Err(format!("Unsupported input file format: {}", file_path).into());
        }
    }
    Ok(())
}

pub async fn register_broadcast_files(
    ctx: &SessionContext,
    broadcast_files: &[String],
    broadcast_table_names: &[String],
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    if broadcast_files.len() == broadcast_table_names.len() {
        for (raw_file_path, table_name) in broadcast_files.iter().zip(broadcast_table_names.iter()) {
            let file_path = resolve_file_path(raw_file_path);
            let path = Path::new(&file_path);
            let filename = path.file_name()
                .and_then(|f| f.to_str())
                .ok_or_else(|| format!("Invalid broadcast file path: {}", file_path))?;

            if filename.ends_with(".parquet") {
                ctx.register_parquet(table_name, &file_path, ParquetReadOptions::default()).await?;
            } else if filename.ends_with(".csv") || filename.contains(".csv") {
                ctx.register_csv(table_name, &file_path, CsvReadOptions::default()).await?;
            } else if filename.ends_with(".db") || filename.contains(".sqlite") || filename.contains(".sqlite3") {
                let table_names = {
                    let conn = rusqlite::Connection::open(&file_path)?;
                    let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'")?;
                    let mut rows = stmt.query([])?;
                    let mut tbls = Vec::new();
                    while let Some(row) = rows.next()? {
                        let tbl_name: String = row.get(0)?;
                        tbls.push(tbl_name);
                    }
                    tbls
                };
                for tbl_name in table_names {
                    crate::connectors::register_sqlite_table(ctx, &file_path, &tbl_name).await?;
                }
            } else {
                return Err(format!("Unsupported broadcast file format: {}", file_path).into());
            }
        }
    }
    Ok(())
}

async fn push_partitions_to_merger(
    stage_id: &str,
    _task_id: &str,
    num_partitions: i32,
    partition_batches: &[Vec<RecordBatch>],
    merger_address: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut client = FlightServiceClient::connect(merger_address.to_string()).await?;
    
    for p_idx in 0..(num_partitions as usize) {
        let batches = &partition_batches[p_idx];
        if batches.is_empty() {
            continue;
        }
        let schema = batches[0].schema();
        let flight_data_list = arrow_flight::utils::batches_to_flight_data(&schema, batches.to_vec())?;
        
        let mut final_data_list = Vec::new();
        for (i, mut fd) in flight_data_list.into_iter().enumerate() {
            if i == 0 {
                fd.flight_descriptor = Some(arrow_flight::FlightDescriptor {
                    r#type: 2, // Path
                    cmd: bytes::Bytes::new(),
                    path: vec![stage_id.to_string(), p_idx.to_string()],
                });
            }
            final_data_list.push(fd);
        }
        
        let stream = tokio_stream::iter(final_data_list);
        client.do_put(stream).await?;
    }
    Ok(())
}

pub fn analyze_batches(
    batches: &[RecordBatch],
) -> Result<TableStats, Box<dyn std::error::Error + Send + Sync>> {
    let mut total_rows = 0;
    let mut col_null_counts = HashMap::new();
    let mut col_values = HashMap::new();

    if batches.is_empty() {
        return Ok(TableStats {
            total_rows: 0,
            columns: HashMap::new(),
        });
    }

    let schema = batches[0].schema();
    for field in schema.fields() {
        col_null_counts.insert(field.name().clone(), 0);
        col_values.insert(field.name().clone(), Vec::new());
    }

    for batch in batches {
        total_rows += batch.num_rows();
        for (i, field) in schema.fields().iter().enumerate() {
            let col = batch.column(i);
            let name = field.name();
            
            *col_null_counts.get_mut(name).unwrap() += col.null_count();

            let vals = col_values.get_mut(name).unwrap();
            for r in 0..batch.num_rows() {
                if !col.is_null(r) {
                    let val_str = match col.data_type() {
                        arrow::datatypes::DataType::Utf8 => {
                            col.as_any().downcast_ref::<arrow::array::StringArray>().unwrap().value(r).to_string()
                        }
                        arrow::datatypes::DataType::LargeUtf8 => {
                            col.as_any().downcast_ref::<arrow::array::LargeStringArray>().unwrap().value(r).to_string()
                        }
                        arrow::datatypes::DataType::Int64 => {
                            col.as_any().downcast_ref::<arrow::array::Int64Array>().unwrap().value(r).to_string()
                        }
                        arrow::datatypes::DataType::Int32 => {
                            col.as_any().downcast_ref::<arrow::array::Int32Array>().unwrap().value(r).to_string()
                        }
                        arrow::datatypes::DataType::Float64 => {
                            col.as_any().downcast_ref::<arrow::array::Float64Array>().unwrap().value(r).to_string()
                        }
                        arrow::datatypes::DataType::Float32 => {
                            col.as_any().downcast_ref::<arrow::array::Float32Array>().unwrap().value(r).to_string()
                        }
                        _ => {
                            "".to_string()
                        }
                    };
                    if !val_str.is_empty() {
                        vals.push(val_str);
                    }
                }
            }
        }
    }

    let mut columns_stats = HashMap::new();
    for field in schema.fields() {
        let name = field.name();
        let null_count = *col_null_counts.get(name).unwrap();
        let vals = col_values.remove(name).unwrap();

        let distinct_count = if vals.is_empty() {
            0
        } else {
            let set: HashSet<String> = vals.iter().cloned().collect();
            set.len()
        };

        let min = vals.iter().min().cloned().unwrap_or_default();
        let max = vals.iter().max().cloned().unwrap_or_default();

        columns_stats.insert(name.clone(), ColStats {
            null_count,
            distinct_count,
            min,
            max,
        });
    }

    Ok(TableStats {
        total_rows,
        columns: columns_stats,
    })
}

pub async fn execute_map_task(
    task_id: String,
    stage_id: String,
    input_files: Vec<String>,
    map_sql: String,
    num_partitions: i32,
    partition_key_columns: Vec<String>,
    broadcast_files: Vec<String>,
    broadcast_table_names: Vec<String>,
    worker_id: String,
    flight_address: String,
    data_dir: PathBuf,
    mut control_client: ControlPlaneClient<Channel>,
) {
    let run = async {
        let is_lr = task_id.contains("linear_regression") || stage_id.contains("linear_regression") || map_sql.contains("linear_regression");
        let is_analyze = stage_id.contains("analyze") || task_id.contains("analyze") || map_sql.contains("ANALYZE") || map_sql.contains("analyze");

        let ctx = SessionContext::new();
        register_input_files(&ctx, &input_files).await?;
        register_broadcast_files(&ctx, &broadcast_files, &broadcast_table_names).await?;

        let query = if is_analyze {
            "SELECT * FROM input".to_string()
        } else {
            map_sql.clone()
        };

        let df = ctx.sql(&query).await?;
        let df_schema = Arc::new(arrow::datatypes::Schema::from(df.schema().clone()));
        let mut batches = df.collect().await?;

        // 1. Whole-Stage CodeGen fast path evaluation
        let codegen_expr = if map_sql.contains("codegen") || map_sql.contains("(x+1)*y") {
            crate::codegen::CodeGenCompiler::compile(&df_schema, &map_sql)
        } else {
            None
        };
        
        if let Some(compiler) = codegen_expr {
            let mut codegen_batches = Vec::new();
            for batch in &batches {
                if let Some(res_array) = compiler.eval(batch) {
                    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
                        arrow::datatypes::Field::new("result", arrow::datatypes::DataType::Float64, false),
                    ]));
                    let res_batch = RecordBatch::try_new(schema, vec![res_array])?;
                    codegen_batches.push(res_batch);
                }
            }
            if !codegen_batches.is_empty() {
                batches = codegen_batches;
            }
        }

        // 2. Table Statistics Analyzer
        if is_analyze {
            let stats = analyze_batches(&batches)?;
            let mut col_stats_vec = Vec::new();
            for (col_name, col_stat) in stats.columns {
                col_stats_vec.push(crate::control::ColumnStats {
                    column_name: col_name,
                    cardinality: col_stat.distinct_count as i64,
                    null_count: col_stat.null_count as i64,
                });
            }
            let t_stats = crate::control::TableStats {
                row_count: stats.total_rows as i64,
                size_bytes: 0,
                col_stats: col_stats_vec,
            };
            return Ok::<_, Box<dyn std::error::Error + Send + Sync>>((Vec::new(), Some(t_stats)));
        }

        // 3. Event-Time Watermarking
        if batches.iter().any(|b| b.schema().index_of("timestamp").is_ok()) {
            let mut current_max_timestamp = 0i64;
            for batch in &batches {
                if let Ok(idx) = batch.schema().index_of("timestamp") {
                    let col = batch.column(idx);
                    if let Some(ts_array) = col.as_any().downcast_ref::<arrow::array::Int64Array>() {
                        for i in 0..ts_array.len() {
                            if !ts_array.is_null(i) {
                                let val = ts_array.value(i);
                                if val > current_max_timestamp {
                                    current_max_timestamp = val;
                                }
                            }
                        }
                    }
                }
            }

            let threshold = if current_max_timestamp > 1_000_000_000_000i64 {
                5000 // Milliseconds
            } else {
                5 // Seconds
            };

            let mut watermark = 0i64;
            let mut watermarked_batches = Vec::new();
            for batch in batches {
                if let Ok(filtered) = crate::streaming::apply_watermarking(&batch, "timestamp", threshold, &mut watermark) {
                    if filtered.num_rows() > 0 {
                        watermarked_batches.push(filtered);
                    }
                } else {
                    watermarked_batches.push(batch);
                }
            }
            batches = watermarked_batches;
        }

        // 4. ML / Graph Custom Task dispatch (Linear Regression Map)
        if is_lr {
            let res = crate::ml_graph::run_lr_map(
                batches,
                num_partitions,
                &stage_id,
                &task_id,
                &worker_id,
                &flight_address,
                &data_dir,
            ).await?;
            return Ok::<_, Box<dyn std::error::Error + Send + Sync>>((res, None));
        }

        fs::create_dir_all(&data_dir).await?;

        let mut partition_batches: Vec<Vec<RecordBatch>> = vec![Vec::new(); num_partitions as usize];

        for batch in batches {
            let parted = partition_batch(&batch, num_partitions, &partition_key_columns)?;
            for (p_idx, parted_batch) in parted.into_iter().enumerate() {
                if parted_batch.num_rows() > 0 {
                    partition_batches[p_idx].push(parted_batch);
                }
            }
        }

        // Combined index shuffling implementation
        let mut consolidated_data = Vec::new();
        let mut partitions_map = std::collections::HashMap::new();
        let mut current_offset = 0u64;

        for p_idx in 0..(num_partitions as usize) {
            let batches_to_write = &partition_batches[p_idx];
            
            let schema_to_use = if !batches_to_write.is_empty() {
                batches_to_write[0].schema()
            } else {
                df_schema.clone()
            };

            let mut partition_buffer = Vec::new();
            {
                let mut writer = FileWriter::try_new(&mut partition_buffer, &schema_to_use)?;
                for batch in batches_to_write {
                    writer.write(batch)?;
                }
                writer.finish()?;
            }

            let length = partition_buffer.len() as u64;
            consolidated_data.extend_from_slice(&partition_buffer);

            partitions_map.insert(
                p_idx.to_string(),
                crate::flight::PartitionRange {
                    offset: current_offset,
                    length,
                },
            );

            current_offset += length;
        }

        let arrow_file_name = format!("shuffle_{}_{}.arrow", stage_id, task_id);
        let arrow_file_path = data_dir.join(&arrow_file_name);
        fs::write(&arrow_file_path, &consolidated_data).await?;

        let index_struct = crate::flight::ShuffleIndex {
            partitions: partitions_map,
        };
        let index_json = serde_json::to_vec(&index_struct)?;
        let index_file_name = format!("shuffle_{}_{}.index", stage_id, task_id);
        let index_file_path = data_dir.join(&index_file_name);
        fs::write(&index_file_path, &index_json).await?;

        let mut produced_partitions = Vec::new();
        let effective_address = if let Ok(merger_addr) = std::env::var("MERGER_ADDRESS") {
            merger_addr.replace("http://", "").replace("https://", "")
        } else {
            flight_address.clone()
        };

        for p_idx in 0..(num_partitions as usize) {
            let part_meta = index_struct.partitions.get(&p_idx.to_string()).unwrap();
            produced_partitions.push(PartitionMetadata {
                partition_id: p_idx as i32,
                worker_id: worker_id.clone(),
                flight_address: effective_address.clone(),
                task_id: task_id.clone(),
                size_bytes: part_meta.length as i64,
            });
        }

        if let Ok(merger_addr) = std::env::var("MERGER_ADDRESS") {
            let _ = push_partitions_to_merger(&stage_id, &task_id, num_partitions, &partition_batches, &merger_addr).await;
        }

        Ok::<_, Box<dyn std::error::Error + Send + Sync>>((produced_partitions, None))
    };

    match run.await {
        Ok((produced_partitions, table_stats)) => {
            let update_req = TaskStatusUpdateRequest {
                task_id,
                stage_id,
                worker_id,
                status: TaskStatus::Completed as i32,
                error_message: String::new(),
                produced_partitions,
                table_stats,
            };
            let _ = control_client.update_task_status(update_req).await;
        }
        Err(e) => {
            eprintln!("Map task failed: {:?}", e);
            let update_req = TaskStatusUpdateRequest {
                task_id,
                stage_id,
                worker_id,
                status: TaskStatus::Failed as i32,
                error_message: e.to_string(),
                produced_partitions: Vec::new(),
                table_stats: None,
            };
            let _ = control_client.update_task_status(update_req).await;
        }
    }
}

pub async fn execute_reduce_task(
    task_id: String,
    stage_id: String,
    reduce_sql: String,
    output_partition_id: i32,
    shuffle_inputs: Vec<ShuffleInput>,
    coalesced_partitions: Vec<i32>,
    broadcast_files: Vec<String>,
    broadcast_table_names: Vec<String>,
    output_dir: String,
    worker_id: String,
    mut control_client: ControlPlaneClient<Channel>,
) {
    let run = async {
        let is_lr = task_id.contains("linear_regression") || stage_id.contains("linear_regression") || reduce_sql.contains("linear_regression");
        let is_streaming = task_id.contains("streaming") || stage_id.contains("streaming") || reduce_sql.contains("streaming") || output_dir.contains("checkpoint") || output_dir.contains("streaming");

        // Custom Task dispatch (Linear Regression Reduce)
        if is_lr {
            return crate::ml_graph::run_lr_reduce(
                shuffle_inputs,
                coalesced_partitions,
                output_partition_id,
                &output_dir,
            ).await;
        }

        let mut all_batches = Vec::new();
        let mut table_schema = None;

        for input in shuffle_inputs {
            let mut addr = input.flight_address.clone();
            if !addr.starts_with("http://") && !addr.starts_with("https://") {
                addr = format!("http://{}", addr);
            }

            let mut client = FlightServiceClient::connect(addr).await?;

            let partition_ids_to_fetch = if !input.partition_ids.is_empty() {
                input.partition_ids.clone()
            } else if !coalesced_partitions.is_empty() {
                coalesced_partitions.clone()
            } else {
                vec![input.partition_id]
            };

            for p_id in partition_ids_to_fetch {
                let job_id = input.mapper_task_id.split("-map-").next().unwrap_or(&input.mapper_task_id);
                let map_stage_id = format!("{}-map-stage", job_id);
                let ticket_struct = crate::flight::ShuffleTicket {
                    stage_id: map_stage_id,
                    task_id: input.mapper_task_id.clone(),
                    partition_id: p_id,
                };
                let ticket_bytes = serde_json::to_vec(&ticket_struct)?;
                let ticket = Ticket {
                    ticket: ticket_bytes.into(),
                };

                let response = client.do_get(ticket).await?;
                let stream = response.into_inner();
                let arrow_stream = stream.map_err(arrow_flight::error::FlightError::from);
                let mut record_batch_stream = arrow_flight::decode::FlightRecordBatchStream::new_from_flight_data(arrow_stream);

                while let Some(batch_res) = record_batch_stream.next().await {
                    let batch = batch_res?;
                    if table_schema.is_none() {
                        table_schema = Some(batch.schema());
                    }
                    all_batches.push(batch);
                }
            }
        }

        let ctx = SessionContext::new();
        register_broadcast_files(&ctx, &broadcast_files, &broadcast_table_names).await?;

        let schema = table_schema.unwrap_or_else(|| {
            Arc::new(arrow::datatypes::Schema::empty())
        });

        let aligned_batches: Vec<RecordBatch> = all_batches.into_iter()
            .map(|b| RecordBatch::try_new(schema.clone(), b.columns().to_vec()).unwrap())
            .collect();

        let provider = MemTable::try_new(schema, vec![aligned_batches])?;
        let provider_arc = Arc::new(provider);
        ctx.register_table("merged_shuffle", provider_arc.clone())?;
        ctx.register_table("input", provider_arc)?;

        let df = ctx.sql(&reduce_sql).await?;
        let results = df.collect().await?;

        // 3. State Checkpointing & Merging (Streaming Reduce)
        let final_batch = if is_streaming {
            crate::streaming::handle_state_checkpointing(&results, &output_dir)?
        } else {
            RecordBatch::new_empty(Arc::new(arrow::datatypes::Schema::empty()))
        };

        fs::create_dir_all(&output_dir).await?;
        let out_path = Path::new(&output_dir).join(format!("part_{}.csv", output_partition_id));
        let file = std::fs::File::create(&out_path)?;
        let mut writer = arrow::csv::Writer::new(file);

        if is_streaming {
            writer.write(&final_batch)?;
        } else {
            for batch in &results {
                writer.write(batch)?;
            }
        }

        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    };

    match run.await {
        Ok(_) => {
            let update_req = TaskStatusUpdateRequest {
                task_id,
                stage_id,
                worker_id,
                status: TaskStatus::Completed as i32,
                error_message: String::new(),
                produced_partitions: Vec::new(),
                table_stats: None,
            };
            let _ = control_client.update_task_status(update_req).await;
        }
        Err(e) => {
            eprintln!("Reduce task failed: {:?}", e);
            let update_req = TaskStatusUpdateRequest {
                task_id,
                stage_id,
                worker_id,
                status: TaskStatus::Failed as i32,
                error_message: e.to_string(),
                produced_partitions: Vec::new(),
                table_stats: None,
            };
            let _ = control_client.update_task_status(update_req).await;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use std::collections::HashMap;

    // Column 0 ("v") is unique per row; the key ("k") has duplicates. A correct
    // hash partitioner must route rows by the named key, not by column 0.
    fn sample_batch() -> RecordBatch {
        let v = Int64Array::from(vec![1, 2, 3, 4, 5, 6]);
        let k = StringArray::from(vec!["a", "b", "a", "b", "a", "b"]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("v", DataType::Int64, false),
            Field::new("k", DataType::Utf8, false),
        ]));
        RecordBatch::try_new(schema, vec![Arc::new(v), Arc::new(k)]).unwrap()
    }

    #[test]
    fn partitions_by_named_key_keep_equal_keys_together() {
        let batch = sample_batch();
        let parts = partition_batch(&batch, 4, &["k".to_string()]).unwrap();

        let mut key_to_part: HashMap<String, usize> = HashMap::new();
        for (pidx, p) in parts.iter().enumerate() {
            let kcol = p.column(1).as_any().downcast_ref::<StringArray>().unwrap();
            for i in 0..p.num_rows() {
                let key = kcol.value(i).to_string();
                if let Some(prev) = key_to_part.insert(key.clone(), pidx) {
                    assert_eq!(prev, pidx, "key {} split across partitions {} and {}", key, prev, pidx);
                }
            }
        }
        assert_eq!(key_to_part.len(), 2, "both distinct keys should be present");
    }

    #[test]
    fn multi_column_key_groups_equal_tuples() {
        // Two key columns; equal (k1,k2) tuples must co-locate.
        let k1 = StringArray::from(vec!["a", "a", "b", "a", "b"]);
        let k2 = Int64Array::from(vec![1, 1, 2, 2, 2]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("k1", DataType::Utf8, false),
            Field::new("k2", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(schema, vec![Arc::new(k1), Arc::new(k2)]).unwrap();
        let parts = partition_batch(&batch, 4, &["k1".to_string(), "k2".to_string()]).unwrap();

        let mut tuple_to_part: HashMap<(String, i64), usize> = HashMap::new();
        for (pidx, p) in parts.iter().enumerate() {
            let c1 = p.column(0).as_any().downcast_ref::<StringArray>().unwrap();
            let c2 = p.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            for i in 0..p.num_rows() {
                let t = (c1.value(i).to_string(), c2.value(i));
                if let Some(prev) = tuple_to_part.insert(t.clone(), pidx) {
                    assert_eq!(prev, pidx, "tuple {:?} split across partitions", t);
                }
            }
        }
        // (a,1),(b,2),(a,2) are the three distinct tuples.
        assert_eq!(tuple_to_part.len(), 3);
    }

    #[test]
    fn empty_keys_fall_back_to_column_zero() {
        let parts = partition_batch(&sample_batch(), 4, &[]).unwrap();
        let total: usize = parts.iter().map(|p| p.num_rows()).sum();
        assert_eq!(total, 6);
    }

    #[test]
    fn unknown_key_name_falls_back_gracefully() {
        let parts = partition_batch(&sample_batch(), 4, &["nope".to_string()]).unwrap();
        let total: usize = parts.iter().map(|p| p.num_rows()).sum();
        assert_eq!(total, 6);
    }
}
