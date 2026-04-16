use crate::QueryError;
use crate::StringComparisonMode;
use crate::text::compare_text_case_insensitive;
use query_sheets_core::{Row, Schema, Value};
use sqlparser::ast::{BinaryOperator, DataType, Expr, Ident, UnaryOperator, Value as SqlValue};

pub(crate) fn resolve_column(schema: &Schema, identifier: &Ident) -> Result<usize, QueryError> {
    resolve_column_name(schema, &identifier.value)
}

pub(crate) fn resolve_compound_column(schema: &Schema, identifiers: &[Ident]) -> Result<usize, QueryError> {
    let Some(last) = identifiers.last() else {
        return Err(QueryError::ColumnNotFound("".to_string()));
    };

    if identifiers.len() >= 2 {
        let qualifier = &identifiers[identifiers.len() - 2].value;
        let qualified_name = format!("{qualifier}.{}", last.value);

        if let Some(index) = schema
            .columns
            .iter()
            .position(|column| column.name.eq_ignore_ascii_case(&qualified_name))
        {
            return Ok(index);
        }
    }

    resolve_column_name(schema, &last.value)
}

pub(crate) fn eval_predicate(
    expr: &Expr,
    row: &Row,
    schema: &Schema,
    string_comparison_mode: StringComparisonMode,
) -> Result<bool, QueryError> {
    match expr {
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => eval_in_list_predicate(
            inner,
            list,
            *negated,
            row,
            schema,
            string_comparison_mode,
        ),
        Expr::BinaryOp { left, op, right } => match op {
            BinaryOperator::And => {
                Ok(
                    eval_predicate(left, row, schema, string_comparison_mode)?
                        && eval_predicate(right, row, schema, string_comparison_mode)?,
                )
            }
            BinaryOperator::Or => {
                Ok(
                    eval_predicate(left, row, schema, string_comparison_mode)?
                        || eval_predicate(right, row, schema, string_comparison_mode)?,
                )
            }
            BinaryOperator::Eq
            | BinaryOperator::NotEq
            | BinaryOperator::Gt
            | BinaryOperator::Lt
            | BinaryOperator::GtEq
            | BinaryOperator::LtEq => {
                let left_value = eval_value(left, row, schema)?;
                let right_value = eval_value(right, row, schema)?;
                compare_values(op, &left_value, &right_value, string_comparison_mode)
            }
            _ => Err(QueryError::UnsupportedWhere(expr.to_string())),
        },
        Expr::Nested(inner) => eval_predicate(inner, row, schema, string_comparison_mode),
        Expr::Value(SqlValue::Boolean(v)) => Ok(*v),
        _ => Err(QueryError::UnsupportedWhere(expr.to_string())),
    }
}

fn eval_in_list_predicate(
    left_expr: &Expr,
    list: &[Expr],
    negated: bool,
    row: &Row,
    schema: &Schema,
    string_comparison_mode: StringComparisonMode,
) -> Result<bool, QueryError> {
    let left_value = eval_value(left_expr, row, schema)?;
    if matches!(left_value, Value::Null) {
        return Ok(false);
    }

    let mut matched = false;
    let mut has_null_candidate = false;

    for candidate_expr in list {
        let candidate = eval_value(candidate_expr, row, schema)?;

        if matches!(candidate, Value::Null) {
            has_null_candidate = true;
            continue;
        }

        let equals = compare_values(
            &BinaryOperator::Eq,
            &left_value,
            &candidate,
            string_comparison_mode,
        )?;

        if equals {
            matched = true;
            break;
        }
    }

    if negated {
        if matched || has_null_candidate {
            return Ok(false);
        }

        return Ok(true);
    }

    Ok(matched)
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
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            let value = eval_value(inner, row, schema)?;
            cast_value(value, data_type)
        }
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

pub(crate) fn is_supported_cast_data_type(data_type: &DataType) -> bool {
    cast_target_type(data_type).is_some()
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
    if let Some(index) = schema.index_of(column_name) {
        return Ok(index);
    }

    let suffix_matches = schema
        .columns
        .iter()
        .enumerate()
        .filter_map(|(idx, column)| {
            let (_, suffix) = column.name.rsplit_once('.')?;
            if suffix.eq_ignore_ascii_case(column_name) {
                Some(idx)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    match suffix_matches.as_slice() {
        [single] => Ok(*single),
        [] => Err(QueryError::ColumnNotFound(column_name.to_string())),
        _ => Err(QueryError::AmbiguousColumn(column_name.to_string())),
    }
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

fn compare_values(
    op: &BinaryOperator,
    left: &Value,
    right: &Value,
    string_comparison_mode: StringComparisonMode,
) -> Result<bool, QueryError> {
    if matches!(left, Value::Null) || matches!(right, Value::Null) {
        // SQL-style behavior for WHERE: comparisons involving NULL evaluate to unknown,
        // and unknown predicates do not pass the filter.
        return Ok(false);
    }

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
        (Value::String(a), Value::String(b)) => {
            let ordering = match string_comparison_mode {
                StringComparisonMode::CaseInsensitive => compare_text_case_insensitive(a, b),
                StringComparisonMode::CaseSensitive => a.cmp(b),
            };

            Ok(compare_ordering(op, ordering))
        }
        (Value::Bool(a), Value::Bool(b)) => Ok(compare_ordering(op, a.cmp(b))),
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

#[derive(Debug, Clone, Copy)]
enum CastTargetType {
    Integer,
    Float,
    String,
    Bool,
}

fn cast_target_type(data_type: &DataType) -> Option<CastTargetType> {
    let normalized = data_type.to_string().to_ascii_uppercase();

    if normalized.contains("CHAR")
        || normalized.contains("TEXT")
        || normalized.contains("STRING")
    {
        return Some(CastTargetType::String);
    }

    if normalized.contains("BOOL") {
        return Some(CastTargetType::Bool);
    }

    if normalized.contains("FLOAT")
        || normalized.contains("DOUBLE")
        || normalized.contains("REAL")
        || normalized.contains("DECIMAL")
        || normalized.contains("NUMERIC")
        || normalized.contains("NUMBER")
    {
        return Some(CastTargetType::Float);
    }

    if normalized.contains("INT") && !normalized.contains("INTERVAL") {
        return Some(CastTargetType::Integer);
    }

    None
}

fn cast_value(value: Value, data_type: &DataType) -> Result<Value, QueryError> {
    let target = cast_target_type(data_type).ok_or_else(|| {
        QueryError::UnsupportedWhere(format!("unsupported cast target type: {data_type}"))
    })?;

    if matches!(value, Value::Null) {
        return Ok(Value::Null);
    }

    match target {
        CastTargetType::Integer => Ok(cast_to_int(value)),
        CastTargetType::Float => Ok(cast_to_float(value)),
        CastTargetType::String => Ok(cast_to_string(value)),
        CastTargetType::Bool => Ok(cast_to_bool(value)),
    }
}

fn cast_to_int(value: Value) -> Value {
    match value {
        Value::Int(v) => Value::Int(v),
        Value::Float(v) => {
            if !v.is_finite() {
                return Value::Null;
            }

            if v < i64::MIN as f64 || v > i64::MAX as f64 {
                return Value::Null;
            }

            Value::Int(v.trunc() as i64)
        }
        Value::String(v) => {
            let trimmed = v.trim();

            if let Ok(parsed) = trimmed.parse::<i64>() {
                return Value::Int(parsed);
            }

            if let Ok(parsed) = trimmed.parse::<f64>() {
                if parsed.is_finite() && parsed >= i64::MIN as f64 && parsed <= i64::MAX as f64 {
                    return Value::Int(parsed.trunc() as i64);
                }
            }

            Value::Null
        }
        Value::Bool(v) => Value::Int(if v { 1 } else { 0 }),
        Value::Null => Value::Null,
    }
}

fn cast_to_float(value: Value) -> Value {
    match value {
        Value::Int(v) => Value::Float(v as f64),
        Value::Float(v) => Value::Float(v),
        Value::String(v) => {
            let trimmed = v.trim();
            trimmed
                .parse::<f64>()
                .map(Value::Float)
                .unwrap_or(Value::Null)
        }
        Value::Bool(v) => Value::Float(if v { 1.0 } else { 0.0 }),
        Value::Null => Value::Null,
    }
}

fn cast_to_string(value: Value) -> Value {
    match value {
        Value::Int(v) => Value::String(v.to_string()),
        Value::Float(v) => Value::String(v.to_string()),
        Value::String(v) => Value::String(v),
        Value::Bool(v) => Value::String(v.to_string()),
        Value::Null => Value::Null,
    }
}

fn cast_to_bool(value: Value) -> Value {
    match value {
        Value::Bool(v) => Value::Bool(v),
        Value::Int(v) => Value::Bool(v != 0),
        Value::Float(v) => {
            if !v.is_finite() {
                return Value::Null;
            }

            Value::Bool(v != 0.0)
        }
        Value::String(v) => {
            let normalized = v.trim().to_ascii_lowercase();

            match normalized.as_str() {
                "true" | "t" | "yes" | "y" | "1" => Value::Bool(true),
                "false" | "f" | "no" | "n" | "0" => Value::Bool(false),
                _ => Value::Null,
            }
        }
        Value::Null => Value::Null,
    }
}