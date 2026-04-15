use crate::QueryError;
use query_sheets_core::{Row, Schema, Value};
use sqlparser::ast::{BinaryOperator, Expr, Ident, UnaryOperator, Value as SqlValue};

pub(crate) fn resolve_column(schema: &Schema, identifier: &Ident) -> Result<usize, QueryError> {
    resolve_column_name(schema, &identifier.value)
}

pub(crate) fn resolve_compound_column(schema: &Schema, identifiers: &[Ident]) -> Result<usize, QueryError> {
    let Some(last) = identifiers.last() else {
        return Err(QueryError::ColumnNotFound("".to_string()));
    };

    resolve_column_name(schema, &last.value)
}

pub(crate) fn eval_predicate(expr: &Expr, row: &Row, schema: &Schema) -> Result<bool, QueryError> {
    match expr {
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                Ok(eval_predicate(left, row, schema)? && eval_predicate(right, row, schema)?)
            }
            BinaryOperator::Or => {
                Ok(eval_predicate(left, row, schema)? || eval_predicate(right, row, schema)?)
            }
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq => {
                let left_value = eval_value(left, row, schema)?;
                let right_value = eval_value(right, row, schema)?;
                compare_values(op, &left_value, &right_value)
            }
            _ => Err(QueryError::UnsupportedWhere(expr.to_string())),
        },
        Expr::Nested(inner) => eval_predicate(inner, row, schema),
        Expr::Value(SqlValue::Boolean(v)) => Ok(*v),
        _ => Err(QueryError::UnsupportedWhere(expr.to_string())),
    }
}

pub(crate) fn eval_value(expr: &Expr, row: &Row, schema: &Schema) -> Result<Value, QueryError> {
    match expr {
        Expr::Identifier(identifier) => {
            let idx = resolve_column(schema, identifier)?;
            Ok(row.values.get(idx).cloned().unwrap_or(Value::Null))
        }
        Expr::CompoundIdentifier(identifiers) => {
            let idx = resolve_compound_column(schema, identifiers)?;
            Ok(row.values.get(idx).cloned().unwrap_or(Value::Null))
        }
        Expr::Value(value) => sql_literal_to_value(value),
        Expr::Nested(inner) => eval_value(inner, row, schema),
        Expr::UnaryOp { op, expr: inner } => {
            let value = eval_value(inner, row, schema)?;
            match (op, value) {
                (UnaryOperator::Plus, Value::Int(v)) => Ok(Value::Int(v)),
                (UnaryOperator::Plus, Value::Float(v)) => Ok(Value::Float(v)),
                (UnaryOperator::Minus, Value::Int(v)) => Ok(Value::Int(-v)),
                (UnaryOperator::Minus, Value::Float(v)) => Ok(Value::Float(-v)),
                _ => Err(QueryError::UnsupportedWhere(expr.to_string())),
            }
        }
        Expr::BinaryOp { left, op, right }
            if matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo
            ) =>
        {
            let left = eval_value(left, row, schema)?;
            let right = eval_value(right, row, schema)?;
            eval_arithmetic_value(op, left, right)
        }
        _ => Err(QueryError::UnsupportedWhere(expr.to_string())),
    }
}

pub(crate) fn sql_literal_to_value(value: &SqlValue) -> Result<Value, QueryError> {
    match value {
        SqlValue::Number(number, _) => {
            if let Ok(v) = number.parse::<i64>() {
                return Ok(Value::Int(v));
            }

            if let Ok(v) = number.parse::<f64>() {
                return Ok(Value::Float(v));
            }

            Err(QueryError::UnsupportedWhere(value.to_string()))
        }
        SqlValue::SingleQuotedString(text) | SqlValue::DoubleQuotedString(text) => {
            Ok(Value::String(text.clone()))
        }
        SqlValue::Boolean(v) => Ok(Value::Bool(*v)),
        SqlValue::Null => Ok(Value::Null),
        _ => Err(QueryError::UnsupportedWhere(value.to_string())),
    }
}

fn resolve_column_name(schema: &Schema, column_name: &str) -> Result<usize, QueryError> {
    schema
        .index_of(column_name)
        .ok_or_else(|| QueryError::ColumnNotFound(column_name.to_string()))
}

fn eval_arithmetic_value(op: &BinaryOperator, left: Value, right: Value) -> Result<Value, QueryError> {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => match op {
            BinaryOperator::Plus => Ok(Value::Int(a + b)),
            BinaryOperator::Minus => Ok(Value::Int(a - b)),
            BinaryOperator::Multiply => Ok(Value::Int(a * b)),
            BinaryOperator::Divide => {
                if b == 0 {
                    return Err(QueryError::UnsupportedWhere("division by zero".to_string()));
                }

                if a % b == 0 {
                    Ok(Value::Int(a / b))
                } else {
                    Ok(Value::Float(a as f64 / b as f64))
                }
            }
            BinaryOperator::Modulo => {
                if b == 0 {
                    return Err(QueryError::UnsupportedWhere("modulo by zero".to_string()));
                }
                Ok(Value::Int(a % b))
            }
            _ => Err(QueryError::UnsupportedWhere(op.to_string())),
        },
        (Value::Int(a), Value::Float(b)) => eval_arithmetic_float(op, a as f64, b),
        (Value::Float(a), Value::Int(b)) => eval_arithmetic_float(op, a, b as f64),
        (Value::Float(a), Value::Float(b)) => eval_arithmetic_float(op, a, b),
        _ => Err(QueryError::UnsupportedWhere(format!("cannot evaluate arithmetic '{op}'"))),
    }
}

fn eval_arithmetic_float(op: &BinaryOperator, left: f64, right: f64) -> Result<Value, QueryError> {
    let value = match op {
        BinaryOperator::Plus => left + right,
        BinaryOperator::Minus => left - right,
        BinaryOperator::Multiply => left * right,
        BinaryOperator::Divide => {
            if right == 0.0 {
                return Err(QueryError::UnsupportedWhere("division by zero".to_string()));
            }
            left / right
        }
        BinaryOperator::Modulo => {
            if right == 0.0 {
                return Err(QueryError::UnsupportedWhere("modulo by zero".to_string()));
            }
            left % right
        }
        _ => return Err(QueryError::UnsupportedWhere(op.to_string())),
    };

    Ok(Value::Float(value))
}

fn compare_values(op: &BinaryOperator, left: &Value, right: &Value) -> Result<bool, QueryError> {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => Ok(compare_ordering(op, a.cmp(b))),
        (Value::Float(a), Value::Float(b)) => {
            let ordering = a.partial_cmp(b).ok_or_else(|| QueryError::UnsupportedWhere(format!("{} {} {}", a, op, b)))?;
            Ok(compare_ordering(op, ordering))
        }
        (Value::Int(a), Value::Float(b)) => {
            let ordering = (*a as f64)
                .partial_cmp(b)
                .ok_or_else(|| QueryError::UnsupportedWhere(format!("{} {} {}", a, op, b)))?;
            Ok(compare_ordering(op, ordering))
        }
        (Value::Float(a), Value::Int(b)) => {
            let ordering = a
                .partial_cmp(&(*b as f64))
                .ok_or_else(|| QueryError::UnsupportedWhere(format!("{} {} {}", a, op, b)))?;
            Ok(compare_ordering(op, ordering))
        }
        (Value::String(a), Value::String(b)) => Ok(compare_ordering(op, a.cmp(b))),
        (Value::Bool(a), Value::Bool(b)) => Ok(compare_ordering(op, a.cmp(b))),
        (Value::Null, Value::Null) => Ok(matches!(op, BinaryOperator::Eq | BinaryOperator::GtEq | BinaryOperator::LtEq)),
        _ => Err(QueryError::UnsupportedWhere(format!("cannot compare '{left:?}' and '{right:?}'"))),
    }
}

fn compare_ordering(op: &BinaryOperator, ordering: std::cmp::Ordering) -> bool {
    match op {
        BinaryOperator::Eq => ordering == std::cmp::Ordering::Equal,
        BinaryOperator::NotEq => ordering != std::cmp::Ordering::Equal,
        BinaryOperator::Gt => ordering == std::cmp::Ordering::Greater,
        BinaryOperator::Lt => ordering == std::cmp::Ordering::Less,
        BinaryOperator::GtEq => {
            ordering == std::cmp::Ordering::Greater || ordering == std::cmp::Ordering::Equal
        }
        BinaryOperator::LtEq => {
            ordering == std::cmp::Ordering::Less || ordering == std::cmp::Ordering::Equal
        }
        _ => false,
    }
}