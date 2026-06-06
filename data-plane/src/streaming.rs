use std::path::Path;
use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use arrow::array::Array;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Debug, Clone)]
pub struct AggStateRow {
    pub user_id: String,
    pub category_name: String,
    pub total_ratings: i64,
    pub total_rating_sum: f64,
}

pub fn apply_watermarking(
    batch: &RecordBatch,
    timestamp_col_name: &str,
    threshold_secs: i64,
    current_max_timestamp: &mut i64,
) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
    let col_idx = batch.schema().index_of(timestamp_col_name)?;
    let col = batch.column(col_idx);
    
    let ts_array = col.as_any().downcast_ref::<arrow::array::Int64Array>()
        .ok_or("Timestamp column must be of Int64 type")?;

    for i in 0..ts_array.len() {
        if !ts_array.is_null(i) {
            let val = ts_array.value(i);
            if val > *current_max_timestamp {
                *current_max_timestamp = val;
            }
        }
    }

    let cutoff = *current_max_timestamp - threshold_secs;
    let mut filter_builder = arrow::array::BooleanBuilder::new();
    for i in 0..ts_array.len() {
        if ts_array.is_null(i) {
            filter_builder.append_value(false);
        } else {
            let val = ts_array.value(i);
            filter_builder.append_value(val >= cutoff);
        }
    }
    let filter_array = filter_builder.finish();

    let filtered_batch = arrow::compute::filter_record_batch(batch, &filter_array)?;
    Ok(filtered_batch)
}

fn find_column_index(schema: &arrow::datatypes::Schema, keywords: &[&str]) -> Option<usize> {
    for (idx, field) in schema.fields().iter().enumerate() {
        let name = field.name().to_lowercase();
        for &kw in keywords {
            if name.contains(kw) {
                return Some(idx);
            }
        }
    }
    None
}

fn get_as_string(array: &arrow::array::ArrayRef, idx: usize) -> String {
    if array.data_type().is_numeric() {
        if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Int64Array>() {
            return arr.value(idx).to_string();
        }
        if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Int32Array>() {
            return arr.value(idx).to_string();
        }
    }
    if let Some(arr) = array.as_any().downcast_ref::<arrow::array::StringArray>() {
        return arr.value(idx).to_string();
    }
    "".to_string()
}

fn get_as_i64(array: &arrow::array::ArrayRef, idx: usize) -> i64 {
    if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Int64Array>() {
        return arr.value(idx);
    }
    if let Some(arr) = array.as_any().downcast_ref::<arrow::array::Int32Array>() {
        return arr.value(idx) as i64;
    }
    0
}

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

pub fn handle_state_checkpointing(
    results: &[RecordBatch],
    output_dir: &str,
) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
    let checkpoint_path = Path::new(output_dir).join("checkpoint_state.json");
    let mut state_map: std::collections::HashMap<(String, String), AggStateRow> = std::collections::HashMap::new();

    if checkpoint_path.exists() {
        if let Ok(data) = std::fs::read_to_string(&checkpoint_path) {
            if let Ok(rows) = serde_json::from_str::<Vec<AggStateRow>>(&data) {
                for row in rows {
                    state_map.insert((row.user_id.clone(), row.category_name.clone()), row);
                }
            }
        }
    }

    for batch in results {
        let schema = batch.schema();
        let user_idx = find_column_index(&schema, &["user_id", "user"]).unwrap_or(0);
        let cat_idx = find_column_index(&schema, &["category_name", "category"]).unwrap_or(1);
        let count_idx = find_column_index(&schema, &["total_ratings", "rating_count", "count"]).unwrap_or(2);
        let sum_idx = find_column_index(&schema, &["total_rating_sum", "rating_sum", "sum"]).unwrap_or(3);

        let user_col = batch.column(user_idx);
        let cat_col = batch.column(cat_idx);
        let count_col = batch.column(count_idx);
        let sum_col = batch.column(sum_idx);

        for i in 0..batch.num_rows() {
            let user_id = get_as_string(user_col, i);
            let category_name = get_as_string(cat_col, i);
            let count_val = get_as_i64(count_col, i);
            let sum_val = get_as_f64(sum_col, i);

            let key = (user_id.clone(), category_name.clone());
            let entry = state_map.entry(key).or_insert(AggStateRow {
                user_id,
                category_name,
                total_ratings: 0,
                total_rating_sum: 0.0,
            });
            entry.total_ratings += count_val;
            entry.total_rating_sum += sum_val;
        }
    }

    let updated_rows: Vec<AggStateRow> = state_map.into_values().collect();
    let json_data = serde_json::to_string_pretty(&updated_rows)?;
    std::fs::write(&checkpoint_path, json_data)?;

    let mut user_ids = Vec::new();
    let mut category_names = Vec::new();
    let mut total_ratings = Vec::new();
    let mut total_rating_sums = Vec::new();

    for row in &updated_rows {
        user_ids.push(row.user_id.clone());
        category_names.push(row.category_name.clone());
        total_ratings.push(row.total_ratings);
        total_rating_sums.push(row.total_rating_sum);
    }

    let schema = Arc::new(arrow::datatypes::Schema::new(vec![
        arrow::datatypes::Field::new("user_id", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("category_name", arrow::datatypes::DataType::Utf8, false),
        arrow::datatypes::Field::new("total_ratings", arrow::datatypes::DataType::Int64, false),
        arrow::datatypes::Field::new("total_rating_sum", arrow::datatypes::DataType::Float64, false),
    ]));

    let user_array = arrow::array::StringArray::from(user_ids);
    let cat_array = arrow::array::StringArray::from(category_names);
    let count_array = arrow::array::Int64Array::from(total_ratings);
    let sum_array = arrow::array::Float64Array::from(total_rating_sums);

    let final_batch = RecordBatch::try_new(schema, vec![
        Arc::new(user_array) as Arc<dyn arrow::array::Array>,
        Arc::new(cat_array) as Arc<dyn arrow::array::Array>,
        Arc::new(count_array) as Arc<dyn arrow::array::Array>,
        Arc::new(sum_array) as Arc<dyn arrow::array::Array>,
    ])?;

    Ok(final_batch)
}
