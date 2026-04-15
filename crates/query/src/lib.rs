use query_sheets_core::{DataSource, Row, Schema, Value};
use sqlparser::ast::{
    BinaryOperator, Expr, Ident, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    Value as SqlValue,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum QueryError {
    #[error("failed to parse SQL: {0}")]
    Parse(String),
    #[error("only SELECT statements are supported")]
    UnsupportedStatement,
    #[error("only simple SELECT queries are supported")]
    UnsupportedQuery,
    #[error("query must reference a single table in FROM")]
    MissingFrom,
    #[error("unsupported select expression: {0}")]
    UnsupportedSelect(String),
    #[error("column not found: {0}")]
    ColumnNotFound(String),
    #[error("unsupported WHERE expression: {0}")]
    UnsupportedWhere(String),
}

pub trait QueryEngine {
    fn execute<'a>(
        &self,
        source: &'a dyn DataSource,
        query: &str,
    ) -> Result<Box<dyn Iterator<Item = Row> + 'a>, QueryError>;
}

#[derive(Debug, Default)]
pub struct SqlLikeQueryEngine;

impl QueryEngine for SqlLikeQueryEngine {
    fn execute<'a>(
        &self,
        source: &'a dyn DataSource,
        query: &str,
    ) -> Result<Box<dyn Iterator<Item = Row> + 'a>, QueryError> {
        let parsed_select = parse_select(query)?;
        let projection = build_projection(source.schema(), &parsed_select.projection)?;
        let where_expr = parsed_select.selection;
        let schema = source.schema().clone();

        let iter = source.scan().filter_map(move |row| {
            if let Some(expr) = &where_expr {
                let keep = eval_predicate(expr, &row, &schema).unwrap_or(false);
                if !keep {
                    return None;
                }
            }

            let values = projection
                .iter()
                .map(|idx| row.values.get(*idx).cloned().unwrap_or(Value::Null))
                .collect();

            Some(Row::new(values))
        });

        Ok(Box::new(iter))
    }
}

pub fn extract_table_name(sql: &str) -> Result<Option<String>, QueryError> {
    let select = parse_select(sql)?;
    let Some(table) = select.from.first() else {
        return Ok(None);
    };

    match &table.relation {
        TableFactor::Table { name, .. } => {
            Ok(name.0.last().map(|identifier| identifier.value.clone()))
        }
        _ => Err(QueryError::UnsupportedQuery),
    }
}

fn parse_select(sql: &str) -> Result<Select, QueryError> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).map_err(|err| QueryError::Parse(err.to_string()))?;
    let statement = statements.first().ok_or(QueryError::UnsupportedStatement)?;

    let Statement::Query(query) = statement else {
        return Err(QueryError::UnsupportedStatement);
    };

    select_from_query(query)
}

fn select_from_query(query: &Query) -> Result<Select, QueryError> {
    let SetExpr::Select(select) = &*query.body else {
        return Err(QueryError::UnsupportedQuery);
    };

    if select.from.is_empty() {
        return Err(QueryError::MissingFrom);
    }

    Ok((**select).clone())
}

fn build_projection(schema: &Schema, select_items: &[SelectItem]) -> Result<Vec<usize>, QueryError> {
    if select_items.is_empty() {
        return Err(QueryError::UnsupportedSelect("projection is empty".to_string()));
    }

    let mut projection = Vec::new();

    for item in select_items {
        match item {
            SelectItem::Wildcard(_) => {
                projection.extend(0..schema.columns.len());
            }
            SelectItem::UnnamedExpr(Expr::Identifier(identifier)) => {
                projection.push(resolve_column(schema, identifier)?);
            }
            SelectItem::ExprWithAlias {
                expr: Expr::Identifier(identifier),
                ..
            } => {
                projection.push(resolve_column(schema, identifier)?);
            }
            other => {
                return Err(QueryError::UnsupportedSelect(other.to_string()));
            }
        }
    }

    Ok(projection)
}

fn resolve_column(schema: &Schema, identifier: &Ident) -> Result<usize, QueryError> {
    schema
        .index_of(&identifier.value)
        .ok_or_else(|| QueryError::ColumnNotFound(identifier.value.clone()))
}

fn eval_predicate(expr: &Expr, row: &Row, schema: &Schema) -> Result<bool, QueryError> {
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

fn eval_value(expr: &Expr, row: &Row, schema: &Schema) -> Result<Value, QueryError> {
    match expr {
        Expr::Identifier(identifier) => {
            let idx = resolve_column(schema, identifier)?;
            Ok(row.values.get(idx).cloned().unwrap_or(Value::Null))
        }
        Expr::Value(value) => sql_literal_to_value(value),
        Expr::Nested(inner) => eval_value(inner, row, schema),
        _ => Err(QueryError::UnsupportedWhere(expr.to_string())),
    }
}

fn sql_literal_to_value(value: &SqlValue) -> Result<Value, QueryError> {
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

#[cfg(test)]
mod tests {
    use super::{QueryEngine, SqlLikeQueryEngine};
    use query_sheets_core::{Column, DataSource, Row, Schema, Value};

    struct MockSource {
        schema: Schema,
        rows: Vec<Row>,
    }

    impl DataSource for MockSource {
        fn schema(&self) -> &Schema {
            &self.schema
        }

        fn scan<'a>(&'a self) -> Box<dyn Iterator<Item = Row> + 'a> {
            Box::new(self.rows.iter().cloned())
        }
    }

    #[test]
    fn executes_select_where_projection() {
        let source = MockSource {
            schema: Schema::new(vec![Column::new("name"), Column::new("age")]),
            rows: vec![
                Row::new(vec![Value::String("ana".into()), Value::Int(10)]),
                Row::new(vec![Value::String("bia".into()), Value::Int(20)]),
                Row::new(vec![Value::String("caio".into()), Value::Int(30)]),
            ],
        };

        let engine = SqlLikeQueryEngine;
        let result = engine
            .execute(&source, "SELECT name FROM planilha WHERE age > 15")
            .expect("query should execute")
            .collect::<Vec<_>>();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].values, vec![Value::String("bia".into())]);
        assert_eq!(result[1].values, vec![Value::String("caio".into())]);
    }
}
