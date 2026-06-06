use std::path::Path;
use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use arrow::ipc::writer::FileWriter;
use crate::control::{PartitionMetadata, ShuffleInput};
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::Ticket;
use futures::{StreamExt, TryStreamExt};

fn get_as_f64(array: &arrow::array::ArrayRef, idx: usize) -> f64 {
    if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Float64Array>() {
        return arr.value(idx);
    }
    if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Float32Array>() {
        return arr.value(idx) as f64;
    }
    if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return arr.value(idx) as f64;
    }
    if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Int32Array>() {
        return arr.value(idx) as f64;
    }
    0.0
}

fn solve_linear_system(mut a: Vec<Vec<f64>>, mut b: Vec<f64>) -> Option<Vec<f64>> {
    let n = a.len();
    for i in 0..n {
        let mut max_row = i;
        for k in (i + 1)..n {
            if a[k][i].abs() > a[max_row][i].abs() {
                max_row = k;
            }
        }
        a.swap(i, max_row);
        b.swap(i, max_row);

        if a[i][i].abs() < 1e-12 {
            return None;
        }

        for k in (i + 1)..n {
            let factor = a[k][i] / a[i][i];
            for j in i..n {
                a[k][j] -= factor * a[i][j];
            }
            b[k] -= factor * b[i];
        }
    }

    let mut x = vec![0.0; n];
    for i in (0..n).rev() {
        let mut sum = 0.0;
        for j in (i + 1)..n {
            sum += a[i][j] * x[j];
        }
        x[i] = (b[i] - sum) / a[i][i];
    }
    Some(x)
}

pub async fn run_lr_map(
    batches: Vec<RecordBatch>,
    num_partitions: i32,
    stage_id: &str,
    task_id: &str,
    worker_id: &str,
    flight_address: &str,
    data_dir: &Path,
) -> Result<Vec<PartitionMetadata>, Box<dyn std::error::Error + Send + Sync>> {
    if batches.is_empty() {
        return Err("No data batches to compute Linear Regression on".into());
    }

    let num_cols = batches[0].num_columns();
    if num_cols < 2 {
        return Err("Linear Regression requires at least 2 columns (features + target)".into());
    }
    let d = num_cols - 1;

    let mut xtx = vec![vec![0.0; d]; d];
    let mut xty = vec![0.0; d];

    for batch in batches {
        let num_rows = batch.num_rows();
        let mut x_arrays = Vec::new();
        for c in 0..d {
            x_arrays.push(batch.column(c));
        }
        let y_array = batch.column(d);

        for i in 0..num_rows {
            let mut x_vec = vec![0.0; d];
            for c in 0..d {
                x_vec[c] = get_as_f64(&x_arrays[c], i);
            }
            let y_val = get_as_f64(y_array, i);

            for r in 0..d {
                for c in 0..d {
                    xtx[r][c] += x_vec[r] * x_vec[c];
                }
                xty[r] += x_vec[r] * y_val;
            }
        }
    }

    let mut fields = Vec::new();
    let mut arrays: Vec<Arc<dyn arrow::array::Array>> = Vec::new();

    for c in 0..d {
        let col_name = format!("xtx_{}", c);
        fields.push(arrow::datatypes::Field::new(&col_name, arrow::datatypes::DataType::Float64, false));
        
        let mut col_data = Vec::new();
        for r in 0..d {
            col_data.push(xtx[r][c]);
        }
        arrays.push(Arc::new(arrow::array::Float64Array::from(col_data)));
    }

    fields.push(arrow::datatypes::Field::new("xty", arrow::datatypes::DataType::Float64, false));
    arrays.push(Arc::new(arrow::array::Float64Array::from(xty)));

    let xtx_xty_batch = RecordBatch::try_new(
        Arc::new(arrow::datatypes::Schema::new(fields)),
        arrays,
    )?;

    tokio::fs::create_dir_all(data_dir).await?;

    let mut partition_buffer = Vec::new();
    {
        let mut writer = FileWriter::try_new(&mut partition_buffer, &xtx_xty_batch.schema())?;
        writer.write(&xtx_xty_batch)?;
        writer.finish()?;
    }

    let length = partition_buffer.len() as u64;
    let mut partitions_map = std::collections::HashMap::new();

    partitions_map.insert(
        "0".to_string(),
        crate::flight::PartitionRange {
            offset: 0,
            length,
        },
    );

    for p in 1..num_partitions {
        partitions_map.insert(
            p.to_string(),
            crate::flight::PartitionRange {
                offset: length,
                length: 0,
            },
        );
    }

    let arrow_file_name = format!("shuffle_{}_{}.arrow", stage_id, task_id);
    let arrow_file_path = data_dir.join(&arrow_file_name);
    tokio::fs::write(&arrow_file_path, &partition_buffer).await?;

    let index_struct = crate::flight::ShuffleIndex {
        partitions: partitions_map,
    };
    let index_json = serde_json::to_vec(&index_struct)?;
    let index_file_name = format!("shuffle_{}_{}.index", stage_id, task_id);
    let index_file_path = data_dir.join(&index_file_name);
    tokio::fs::write(&index_file_path, &index_json).await?;

    let mut produced_partitions = Vec::new();
    for p_idx in 0..(num_partitions as usize) {
        let part_meta = index_struct.partitions.get(&p_idx.to_string()).unwrap();
        produced_partitions.push(PartitionMetadata {
            partition_id: p_idx as i32,
            worker_id: worker_id.to_string(),
            flight_address: flight_address.to_string(),
            task_id: task_id.to_string(),
            size_bytes: part_meta.length as i64,
        });
    }

    Ok(produced_partitions)
}

pub async fn run_lr_reduce(
    shuffle_inputs: Vec<ShuffleInput>,
    coalesced_partitions: Vec<i32>,
    output_partition_id: i32,
    output_dir: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut global_xtx: Option<Vec<Vec<f64>>> = None;
    let mut global_xty: Option<Vec<f64>> = None;

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
            let ticket_struct = crate::flight::ShuffleTicket {
                stage_id: "map-stage".to_string(),
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
                let num_rows = batch.num_rows();
                let num_cols = batch.num_columns();
                if num_cols < 2 {
                    continue;
                }
                let d = num_cols - 1;

                if global_xtx.is_none() {
                    global_xtx = Some(vec![vec![0.0; d]; d]);
                    global_xty = Some(vec![0.0; d]);
                }

                let xtx_ref = global_xtx.as_mut().unwrap();
                let xty_ref = global_xty.as_mut().unwrap();

                for r in 0..num_rows {
                    for c in 0..d {
                        let val = get_as_f64(batch.column(c), r);
                        xtx_ref[r][c] += val;
                    }
                    let val = get_as_f64(batch.column(d), r);
                    xty_ref[r] += val;
                }
            }
        }
    }

    let xtx = global_xtx.ok_or("No ML data received")?;
    let xty = global_xty.ok_or("No ML data received")?;

    let theta = solve_linear_system(xtx, xty)
        .ok_or("System is singular (cannot invert X^T X)")?;

    tokio::fs::create_dir_all(output_dir).await?;
    let out_path = Path::new(output_dir).join(format!("part_{}.csv", output_partition_id));
    
    let file = std::fs::File::create(&out_path)?;
    let mut writer = arrow::csv::Writer::new(file);

    let mut idxs = Vec::new();
    let mut vals = Vec::new();
    for (i, val) in theta.iter().enumerate() {
        idxs.push(i as i64);
        vals.push(*val);
    }

    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("coefficient_idx", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("value", arrow::datatypes::DataType::Float64, false),
    ]));

    let idx_array = arrow::array::Int64Array::from(idxs);
    let val_array = arrow::array::Float64Array::from(vals);
    let final_batch = RecordBatch::try_new(schema, vec![
        Arc::new(idx_array) as Arc<dyn arrow::array::Array>,
        Arc::new(val_array) as Arc<dyn arrow::array::Array>,
    ])?;

    writer.write(&final_batch)?;

    Ok(())
}
