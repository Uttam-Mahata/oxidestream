use std::sync::Arc;
use rusqlite::Connection;
use arrow::record_batch::RecordBatch;
use arrow::array::{ArrayRef, StringBuilder, Int64Builder, Float64Builder};
use arrow::datatypes::{Field, Schema, DataType};
use datafusion::prelude::SessionContext;
use datafusion::datasource::MemTable;

pub fn read_sqlite_table(
    db_path: &str,
    table_name: &str,
) -> Result<RecordBatch, Box<dyn std::error::Error + Send + Sync>> {
    let conn = Connection::open(db_path)?;
    let mut stmt = conn.prepare(&format!("SELECT * FROM {}", table_name))?;
    
    let col_names: Vec<String> = stmt.column_names().into_iter().map(|s| s.to_string()).collect();
    let num_cols = col_names.len();
    
    let mut rows = stmt.query([])?;
    
    let mut string_builders: Vec<StringBuilder> = (0..num_cols).map(|_| StringBuilder::new()).collect();
    let mut int_builders: Vec<Int64Builder> = (0..num_cols).map(|_| Int64Builder::new()).collect();
    let mut float_builders: Vec<Float64Builder> = (0..num_cols).map(|_| Float64Builder::new()).collect();
    
    let mut col_derived_types = vec![0; num_cols];
    
    let mut type_stmt = conn.prepare(&format!("PRAGMA table_info({})", table_name))?;
    let mut type_rows = type_stmt.query([])?;
    let col_indices: std::collections::HashMap<String, usize> = col_names.iter().enumerate()
        .map(|(i, name)| (name.to_lowercase(), i))
        .collect();
        
    while let Some(row) = type_rows.next()? {
        let name: String = row.get(1)?;
        let decl_type: String = row.get(2)?;
        if let Some(&idx) = col_indices.get(&name.to_lowercase()) {
            let decl = decl_type.to_uppercase();
            if decl.contains("INT") || decl.contains("BIT") {
                col_derived_types[idx] = 1;
            } else if decl.contains("REAL") || decl.contains("DOUBLE") || decl.contains("FLOAT") || decl.contains("NUM") {
                col_derived_types[idx] = 2;
            } else {
                col_derived_types[idx] = 0;
            }
        }
    }

    while let Some(row) = rows.next()? {
        for i in 0..num_cols {
            match col_derived_types[i] {
                1 => {
                    let val: Option<i64> = row.get(i)?;
                    int_builders[i].append_option(val);
                }
                2 => {
                    let val: Option<f64> = row.get(i)?;
                    float_builders[i].append_option(val);
                }
                _ => {
                    let val: Option<String> = row.get(i)?;
                    string_builders[i].append_option(val.as_deref());
                }
            }
        }
    }

    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();

    for i in 0..num_cols {
        let name = &col_names[i];
        match col_derived_types[i] {
            1 => {
                fields.push(Field::new(name, DataType::Int64, true));
                arrays.push(Arc::new(int_builders[i].finish()) as ArrayRef);
            }
            2 => {
                fields.push(Field::new(name, DataType::Float64, true));
                arrays.push(Arc::new(float_builders[i].finish()) as ArrayRef);
            }
            _ => {
                fields.push(Field::new(name, DataType::Utf8, true));
                arrays.push(Arc::new(string_builders[i].finish()) as ArrayRef);
            }
        }
    }

    let schema = Arc::new(Schema::new(fields));
    let batch = RecordBatch::try_new(schema, arrays)?;
    Ok(batch)
}

pub async fn register_sqlite_table(
    ctx: &SessionContext,
    db_path: &str,
    table_name: &str,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let batch = read_sqlite_table(db_path, table_name)?;
    let schema = batch.schema();
    let provider = MemTable::try_new(schema, vec![vec![batch]])?;
    ctx.register_table(table_name, Arc::new(provider))?;
    Ok(())
}
