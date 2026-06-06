use std::sync::Arc;
use arrow::array::{ArrayRef, Float64Array};
use arrow::compute;
use arrow::record_batch::RecordBatch;

// ---------------------------------------------------------------------------
// Instruction set — unchanged; drives the stack-machine in eval()
// ---------------------------------------------------------------------------

#[derive(Clone, Debug)]
pub enum Instruction {
    LoadCol(usize),
    LoadConst(f64),
    Add,
    Sub,
    Mul,
    Div,
}

// ---------------------------------------------------------------------------
// Tokenizer
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
enum Token {
    Num(f64),
    Ident(String),
    Plus,
    Minus,
    Star,
    Slash,
    LParen,
    RParen,
    Eof,
}

fn tokenize(input: &str) -> Option<Vec<Token>> {
    let mut tokens = Vec::new();
    let chars: Vec<char> = input.chars().collect();
    let mut i = 0;

    while i < chars.len() {
        match chars[i] {
            ' ' | '\t' | '\n' | '\r' => {
                i += 1;
            }
            '+' => { tokens.push(Token::Plus);   i += 1; }
            '-' => { tokens.push(Token::Minus);  i += 1; }
            '*' => { tokens.push(Token::Star);   i += 1; }
            '/' => { tokens.push(Token::Slash);  i += 1; }
            '(' => { tokens.push(Token::LParen); i += 1; }
            ')' => { tokens.push(Token::RParen); i += 1; }
            c if c.is_ascii_alphabetic() || c == '_' => {
                let start = i;
                while i < chars.len()
                    && (chars[i].is_ascii_alphanumeric() || chars[i] == '_')
                {
                    i += 1;
                }
                let name: String = chars[start..i].iter().collect();
                tokens.push(Token::Ident(name));
            }
            c if c.is_ascii_digit() || c == '.' => {
                let start = i;
                while i < chars.len()
                    && (chars[i].is_ascii_digit() || chars[i] == '.')
                {
                    i += 1;
                }
                let num_str: String = chars[start..i].iter().collect();
                let num: f64 = num_str.parse().ok()?;
                tokens.push(Token::Num(num));
            }
            _ => return None, // unexpected character
        }
    }

    tokens.push(Token::Eof);
    Some(tokens)
}

// ---------------------------------------------------------------------------
// Recursive-descent parser
//
// Grammar:
//   expr   → term   { ('+' | '-') term }
//   term   → factor { ('*' | '/') factor }
//   factor → '(' expr ')' | CONST | IDENT
// ---------------------------------------------------------------------------

struct Parser<'a> {
    tokens: &'a [Token],
    pos: usize,
    schema: &'a arrow::datatypes::Schema,
    instructions: Vec<Instruction>,
}

impl<'a> Parser<'a> {
    fn new(tokens: &'a [Token], schema: &'a arrow::datatypes::Schema) -> Self {
        Parser { tokens, pos: 0, schema, instructions: Vec::new() }
    }

    fn peek(&self) -> &Token {
        &self.tokens[self.pos]
    }

    fn consume(&mut self) -> &Token {
        let tok = &self.tokens[self.pos];
        self.pos += 1;
        tok
    }

    // expr → term { ('+' | '-') term }
    fn parse_expr(&mut self) -> Option<()> {
        self.parse_term()?;
        loop {
            match self.peek() {
                Token::Plus => {
                    self.consume();
                    self.parse_term()?;
                    self.instructions.push(Instruction::Add);
                }
                Token::Minus => {
                    self.consume();
                    self.parse_term()?;
                    self.instructions.push(Instruction::Sub);
                }
                _ => break,
            }
        }
        Some(())
    }

    // term → factor { ('*' | '/') factor }
    fn parse_term(&mut self) -> Option<()> {
        self.parse_factor()?;
        loop {
            match self.peek() {
                Token::Star => {
                    self.consume();
                    self.parse_factor()?;
                    self.instructions.push(Instruction::Mul);
                }
                Token::Slash => {
                    self.consume();
                    self.parse_factor()?;
                    self.instructions.push(Instruction::Div);
                }
                _ => break,
            }
        }
        Some(())
    }

    // factor → '(' expr ')' | CONST | IDENT
    fn parse_factor(&mut self) -> Option<()> {
        match self.peek().clone() {
            Token::LParen => {
                self.consume(); // '('
                self.parse_expr()?;
                if self.peek() != &Token::RParen {
                    return None; // mismatched parenthesis
                }
                self.consume(); // ')'
                Some(())
            }
            Token::Num(n) => {
                let n = n; // copy before consume
                self.consume();
                self.instructions.push(Instruction::LoadConst(n));
                Some(())
            }
            Token::Ident(ref name) => {
                let name = name.clone();
                self.consume();
                let idx = self.schema.index_of(&name).ok()?;
                self.instructions.push(Instruction::LoadCol(idx));
                Some(())
            }
            _ => None, // unexpected token
        }
    }
}

// ---------------------------------------------------------------------------
// CodeGenCompiler
// ---------------------------------------------------------------------------

pub struct CodeGenCompiler {
    pub instructions: Vec<Instruction>,
}

impl CodeGenCompiler {
    /// Parse `expr_str` into a bytecode program for the stack machine.
    /// Returns `None` on any parse error or unknown column name so the caller
    /// can fall back to DataFusion.
    pub fn compile(
        schema: &arrow::datatypes::Schema,
        expr_str: &str,
    ) -> Option<Self> {
        let tokens = tokenize(expr_str)?;
        let mut parser = Parser::new(&tokens, schema);
        parser.parse_expr()?;
        // The entire input must be consumed (only Eof should remain)
        if parser.peek() != &Token::Eof {
            return None;
        }
        Some(CodeGenCompiler { instructions: parser.instructions })
    }

    /// Evaluate the compiled expression against every row in `batch`.
    /// Uses a general stack machine so it works for any instruction sequence
    /// produced by `compile()`.
    pub fn eval(&self, batch: &RecordBatch) -> Option<ArrayRef> {
        let num_rows = batch.num_rows();
        let mut stack: Vec<Vec<Option<f64>>> = Vec::new();

        for instr in &self.instructions {
            match instr {
                Instruction::LoadCol(idx) => {
                    let col = batch.column(*idx);
                    let f64_col = compute::cast(
                        col,
                        &arrow::datatypes::DataType::Float64,
                    )
                    .ok()?;
                    let arr = f64_col
                        .as_any()
                        .downcast_ref::<Float64Array>()?;
                    stack.push(arr.iter().collect());
                }
                Instruction::LoadConst(c) => {
                    stack.push(vec![Some(*c); num_rows]);
                }
                Instruction::Add => {
                    let b = stack.pop()?;
                    let a = stack.pop()?;
                    stack.push(
                        a.into_iter()
                            .zip(b.into_iter())
                            .map(|(av, bv)| Some(av? + bv?))
                            .collect(),
                    );
                }
                Instruction::Sub => {
                    let b = stack.pop()?;
                    let a = stack.pop()?;
                    stack.push(
                        a.into_iter()
                            .zip(b.into_iter())
                            .map(|(av, bv)| Some(av? - bv?))
                            .collect(),
                    );
                }
                Instruction::Mul => {
                    let b = stack.pop()?;
                    let a = stack.pop()?;
                    stack.push(
                        a.into_iter()
                            .zip(b.into_iter())
                            .map(|(av, bv)| Some(av? * bv?))
                            .collect(),
                    );
                }
                Instruction::Div => {
                    let b = stack.pop()?;
                    let a = stack.pop()?;
                    stack.push(
                        a.into_iter()
                            .zip(b.into_iter())
                            .map(|(av, bv)| Some(av? / bv?))
                            .collect(),
                    );
                }
            }
        }

        let result = stack.pop()?;
        let arr: Float64Array = result.into_iter().collect();
        Some(Arc::new(arr) as ArrayRef)
    }
}
