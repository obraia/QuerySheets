use std::fmt::{Display, Formatter};

#[derive(Debug, Clone, PartialEq)]
pub struct Column {
    pub name: String,
}

impl Column {
    pub fn new(name: impl Into<String>) -> Self {
        Self { name: name.into() }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Schema {
    pub columns: Vec<Column>,
}

impl Schema {
    pub fn new(columns: Vec<Column>) -> Self {
        Self { columns }
    }

    pub fn index_of(&self, column_name: &str) -> Option<usize> {
        self.columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(column_name))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct Row {
    pub values: Vec<Value>,
}

impl Row {
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Int(i64),
    Float(f64),
    String(String),
    Bool(bool),
    Null,
}

impl Display for Value {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            Value::Int(v) => write!(f, "{v}"),
            Value::Float(v) => write!(f, "{v}"),
            Value::String(v) => write!(f, "{v}"),
            Value::Bool(v) => write!(f, "{v}"),
            Value::Null => write!(f, "NULL"),
        }
    }
}

pub trait DataSource {
    fn schema(&self) -> &Schema;
    fn scan<'a>(&'a self) -> Box<dyn Iterator<Item = Row> + 'a>;
}
