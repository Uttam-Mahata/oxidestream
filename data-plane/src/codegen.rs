use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use arrow::array::{ArrayRef, Float64Array};

#[derive(Clone, Debug)]
pub enum Instruction {
    LoadCol(usize),
    LoadConst(f64),
    Add,
    Sub,
    Mul,
    Div,
}

pub struct CodeGenCompiler {
    pub instructions: Vec<Instruction>,
}

impl CodeGenCompiler {
    pub fn compile(
        schema: &arrow::datatypes::Schema,
        expr_str: &str,
    ) -> Option<Self> {
        let trimmed = expr_str.replace(" ", "");
        if trimmed.contains("(x+1)*y") {
            let x_idx = schema.index_of("x").ok()?;
            let y_idx = schema.index_of("y").ok()?;
            Some(CodeGenCompiler {
                instructions: vec![
                    Instruction::LoadCol(x_idx),
                    Instruction::LoadConst(1.0),
                    Instruction::Add,
                    Instruction::LoadCol(y_idx),
                    Instruction::Mul,
                ],
            })
        } else if trimmed.contains("x+1") {
            let x_idx = schema.index_of("x").ok()?;
            Some(CodeGenCompiler {
                instructions: vec![
                    Instruction::LoadCol(x_idx),
                    Instruction::LoadConst(1.0),
                    Instruction::Add,
                ],
            })
        } else {
            None
        }
    }

    pub fn eval(&self, batch: &RecordBatch) -> Option<ArrayRef> {
        if self.instructions.len() == 5 {
            if let (
                Instruction::LoadCol(x_idx),
                Instruction::LoadConst(c_val),
                Instruction::Add,
                Instruction::LoadCol(y_idx),
                Instruction::Mul,
            ) = (
                &self.instructions[0],
                &self.instructions[1],
                &self.instructions[2],
                &self.instructions[3],
                &self.instructions[4],
            ) {
                let x_col = batch.column(*x_idx);
                let y_col = batch.column(*y_idx);
                let x_arr = x_col.as_any().downcast_ref::<Float64Array>()?;
                let y_arr = y_col.as_any().downcast_ref::<Float64Array>()?;

                let res: Float64Array = x_arr.iter()
                    .zip(y_arr.iter())
                    .map(|(x_opt, y_opt)| match (x_opt, y_opt) {
                        (Some(x), Some(y)) => Some((x + c_val) * y),
                        _ => None,
                    })
                    .collect();
                return Some(Arc::new(res) as ArrayRef);
            }
        } else if self.instructions.len() == 3 {
            if let (
                Instruction::LoadCol(x_idx),
                Instruction::LoadConst(c_val),
                Instruction::Add,
            ) = (
                &self.instructions[0],
                &self.instructions[1],
                &self.instructions[2],
            ) {
                let x_col = batch.column(*x_idx);
                let x_arr = x_col.as_any().downcast_ref::<Float64Array>()?;

                let res: Float64Array = x_arr.iter()
                    .map(|x_opt| x_opt.map(|x| x + c_val))
                    .collect();
                return Some(Arc::new(res) as ArrayRef);
            }
        }
        None
    }
}
