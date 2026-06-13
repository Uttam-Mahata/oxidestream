// Query planner: compiles a single SQL statement into a 2-stage map/reduce
// plan with the correct hash-partition keys, so callers no longer have to
// hand-write `map_sql` + `reduce_sql`.
//
// Increment 1 scope: a single top-level GROUP BY aggregation built from the
// distributive/algebraic aggregates SUM, COUNT, MIN, MAX. The map stage runs
// the user query verbatim (producing partial aggregates); the reduce stage
// re-combines those partials grouped by the same keys. Anything outside this
// shape returns an error so the control plane can fall back to the manual
// map_sql/reduce_sql path.

use datafusion::prelude::*;
use datafusion::logical_expr::{Aggregate, Expr, LogicalPlan};

pub struct PlannedQuery {
    pub map_sql: String,
    pub reduce_sql: String,
    pub partition_key_columns: Vec<String>,
}

pub async fn plan_query(
    sql: &str,
    input_files: &[String],
    _num_partitions: i32,
) -> Result<PlannedQuery, Box<dyn std::error::Error + Send + Sync>> {
    let ctx = SessionContext::new();
    crate::executor::register_input_files(&ctx, input_files).await?;

    let df = ctx.sql(sql).await?;

    // Final output column names exactly as the map stage will materialize them
    // (the same DFSchema -> Arrow Schema conversion the executor uses), so the
    // partition keys and reduce-stage references match the shuffle batches.
    let arrow_schema = arrow::datatypes::Schema::from(df.schema().clone());
    let final_fields: Vec<String> = arrow_schema
        .fields()
        .iter()
        .map(|f| f.name().to_string())
        .collect();

    let plan = df.logical_plan();
    let agg = find_aggregate(plan).ok_or_else(|| {
        "PlanQuery supports a single GROUP BY aggregation; no Aggregate node found".to_string()
    })?;

    let group_len = agg.group_expr.len();
    let aggr_len = agg.aggr_expr.len();

    // The supported shape produces exactly [group keys..., aggregates...] in
    // order. A wrapping projection that changes arity is out of scope.
    if final_fields.len() != group_len + aggr_len {
        return Err(format!(
            "PlanQuery: output columns ({}) != {} group keys + {} aggregates; \
             use manual map_sql/reduce_sql",
            final_fields.len(),
            group_len,
            aggr_len
        )
        .into());
    }

    let key_cols: Vec<String> = final_fields[..group_len].to_vec();

    let mut select_parts: Vec<String> = key_cols.iter().map(|k| quote_ident(k)).collect();
    for (i, aggr) in agg.aggr_expr.iter().enumerate() {
        let op = combine_op(aggr)?;
        let out_name = &final_fields[group_len + i];
        select_parts.push(format!(
            "{}({}) AS {}",
            op,
            quote_ident(out_name),
            quote_ident(out_name)
        ));
    }

    let reduce_sql = if key_cols.is_empty() {
        format!("SELECT {} FROM input", select_parts.join(", "))
    } else {
        let group_by = key_cols
            .iter()
            .map(|k| quote_ident(k))
            .collect::<Vec<_>>()
            .join(", ");
        format!(
            "SELECT {} FROM input GROUP BY {}",
            select_parts.join(", "),
            group_by
        )
    };

    Ok(PlannedQuery {
        map_sql: sql.to_string(),
        reduce_sql,
        partition_key_columns: key_cols,
    })
}

/// Find the first (top-most) Aggregate node in the plan tree.
fn find_aggregate(plan: &LogicalPlan) -> Option<&Aggregate> {
    if let LogicalPlan::Aggregate(agg) = plan {
        return Some(agg);
    }
    for input in plan.inputs() {
        if let Some(agg) = find_aggregate(input) {
            return Some(agg);
        }
    }
    None
}

/// Map a partial aggregate expression to the SQL function that combines those
/// partials in the reduce stage. COUNT combines by summing the partial counts.
fn combine_op(expr: &Expr) -> Result<&'static str, Box<dyn std::error::Error + Send + Sync>> {
    let inner = match expr {
        Expr::Alias(alias) => alias.expr.as_ref(),
        other => other,
    };
    let rendered = format!("{}", inner).to_uppercase();

    if rendered.contains("DISTINCT") {
        return Err(format!(
            "PlanQuery: DISTINCT aggregates cannot be split into partial/final: {}",
            rendered
        )
        .into());
    }

    if rendered.starts_with("SUM(") {
        Ok("SUM")
    } else if rendered.starts_with("COUNT(") {
        Ok("SUM")
    } else if rendered.starts_with("MIN(") {
        Ok("MIN")
    } else if rendered.starts_with("MAX(") {
        Ok("MAX")
    } else {
        Err(format!(
            "PlanQuery: only SUM/COUNT/MIN/MAX can be split into partial/final, got: {}",
            rendered
        )
        .into())
    }
}

/// Double-quote an identifier so output names containing dots, spaces, or
/// parentheses (e.g. "SUM(input.b)") are valid in the reduce SQL.
fn quote_ident(name: &str) -> String {
    format!("\"{}\"", name.replace('"', "\"\""))
}
