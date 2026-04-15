use query_sheets_core::{Column, DataSource, Row, Schema, Value};
use sqlparser::ast::{
    BinaryOperator, Expr, Ident, Query, Select, SelectItem, SetExpr, Statement, TableFactor,
    UnaryOperator, Value as SqlValue,
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
    fn execute_with_schema<'a>(
        &self,
        source: &'a dyn DataSource,
        query: &str,
    ) -> Result<QueryExecution<'a>, QueryError>;

    fn execute<'a>(
        &self,
        source: &'a dyn DataSource,
        query: &str,
    ) -> Result<Box<dyn Iterator<Item = Row> + 'a>, QueryError> {
        Ok(self.execute_with_schema(source, query)?.rows)
    }
}

#[derive(Debug, Default)]
pub struct SqlLikeQueryEngine;

pub struct QueryExecution<'a> {
    pub schema: Schema,
    pub rows: Box<dyn Iterator<Item = Row> + 'a>,
}

#[derive(Debug, Clone)]
enum ProjectionItem {
    Wildcard,
    Expr(Expr),
}

impl QueryEngine for SqlLikeQueryEngine {
    fn execute_with_schema<'a>(
        &self,
        source: &'a dyn DataSource,
        query: &str,
    ) -> Result<QueryExecution<'a>, QueryError> {
        let parsed_select = parse_select(query)?;
        let (projection, projected_schema) = build_projection(source.schema(), &parsed_select.projection)?;
        let where_expr = parsed_select.selection;
        let schema = source.schema().clone();

        let iter = source.scan().filter_map(move |row| {
            if let Some(expr) = &where_expr {
                let keep = eval_predicate(expr, &row, &schema).unwrap_or(false);
                if !keep {
                    return None;
                }
            }

            let values = project_row(&projection, &row, &schema);

            Some(Row::new(values))
        });

        Ok(QueryExecution {
            schema: projected_schema,
            rows: Box::new(iter),
        })
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

fn build_projection(
    schema: &Schema,
    select_items: &[SelectItem],
) -> Result<(Vec<ProjectionItem>, Schema), QueryError> {
    if select_items.is_empty() {
        return Err(QueryError::UnsupportedSelect("projection is empty".to_string()));
    }

    let mut projection = Vec::new();
    let mut output_columns = Vec::new();

    for item in select_items {
        match item {
            SelectItem::Wildcard(_) => {
                projection.push(ProjectionItem::Wildcard);
                output_columns.extend(schema.columns.iter().cloned());
            }
            SelectItem::UnnamedExpr(expr) => {
                validate_projection_expr(schema, expr)?;
                let output_name = projection_output_name(expr);
                projection.push(ProjectionItem::Expr(expr.clone()));
                output_columns.push(Column::new(output_name));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                validate_projection_expr(schema, expr)?;
                projection.push(ProjectionItem::Expr(expr.clone()));
                output_columns.push(Column::new(alias.value.clone()));
            }
            other => {
                return Err(QueryError::UnsupportedSelect(other.to_string()));
            }
        }
    }

    Ok((projection, Schema::new(output_columns)))
}

fn projection_output_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(identifier) => identifier.value.clone(),
        Expr::CompoundIdentifier(identifiers) => identifiers
            .last()
            .map(|ident| ident.value.clone())
            .unwrap_or_else(|| expr.to_string()),
        _ => expr.to_string(),
    }
}

fn resolve_column(schema: &Schema, identifier: &Ident) -> Result<usize, QueryError> {
    resolve_column_name(schema, &identifier.value)
}

fn resolve_column_name(schema: &Schema, column_name: &str) -> Result<usize, QueryError> {
    schema
        .index_of(column_name)
        .ok_or_else(|| QueryError::ColumnNotFound(column_name.to_string()))
}

fn resolve_compound_column(schema: &Schema, identifiers: &[Ident]) -> Result<usize, QueryError> {
    let Some(last) = identifiers.last() else {
        return Err(QueryError::ColumnNotFound("".to_string()));
    };

    resolve_column_name(schema, &last.value)
}

fn validate_projection_expr(schema: &Schema, expr: &Expr) -> Result<(), QueryError> {
    match expr {
        Expr::Identifier(identifier) => {
            let _ = resolve_column(schema, identifier)?;
            Ok(())
        }
        Expr::CompoundIdentifier(identifiers) => {
            let _ = resolve_compound_column(schema, identifiers)?;
            Ok(())
        }
        Expr::Value(value) => {
            let _ = sql_literal_to_value(value).map_err(|_| QueryError::UnsupportedSelect(expr.to_string()))?;
            Ok(())
        }
        Expr::Nested(inner) => validate_projection_expr(schema, inner),
        Expr::UnaryOp { op, expr: inner } => match op {
            UnaryOperator::Plus | UnaryOperator::Minus => validate_projection_expr(schema, inner),
            _ => Err(QueryError::UnsupportedSelect(expr.to_string())),
        },
        Expr::BinaryOp { left, op, right } => {
            if !matches!(
                op,
                BinaryOperator::Plus
                    | BinaryOperator::Minus
                    | BinaryOperator::Multiply
                    | BinaryOperator::Divide
                    | BinaryOperator::Modulo
            ) {
                return Err(QueryError::UnsupportedSelect(expr.to_string()));
            }

            validate_projection_expr(schema, left)?;
            validate_projection_expr(schema, right)
        }
        _ => Err(QueryError::UnsupportedSelect(expr.to_string())),
    }
}

fn project_row(projection: &[ProjectionItem], row: &Row, schema: &Schema) -> Vec<Value> {
    let mut out = Vec::new();

    for item in projection {
        match item {
            ProjectionItem::Wildcard => out.extend(row.values.iter().cloned()),
            ProjectionItem::Expr(expr) => out.push(eval_value(expr, row, schema).unwrap_or(Value::Null)),
        }
    }

    out
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

    #[test]
    fn executes_projection_with_alias_and_expression() {
        let source = MockSource {
            schema: Schema::new(vec![Column::new("name"), Column::new("age")]),
            rows: vec![
                Row::new(vec![Value::String("ana".into()), Value::Int(10)]),
                Row::new(vec![Value::String("bia".into()), Value::Int(20)]),
            ],
        };

        let engine = SqlLikeQueryEngine;
        let result = engine
            .execute(
                &source,
                "SELECT name AS pessoa, age + 5 AS idade_ajustada, 1 AS constante FROM planilha WHERE name = 'bia'",
            )
            .expect("query should execute")
            .collect::<Vec<_>>();

        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].values,
            vec![Value::String("bia".into()), Value::Int(25), Value::Int(1)]
        );
    }

    #[test]
    fn returns_projected_schema_with_aliases() {
        let source = MockSource {
            schema: Schema::new(vec![Column::new("name"), Column::new("age")]),
            rows: vec![Row::new(vec![Value::String("bia".into()), Value::Int(20)])],
        };

        let engine = SqlLikeQueryEngine;
        let execution = engine
            .execute_with_schema(
                &source,
                "SELECT name AS pessoa, age + 1 AS idade_ajustada, age * 2 FROM planilha",
            )
            .expect("query should execute");

        let header = execution
            .schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let rows = execution.rows.collect::<Vec<_>>();

        assert_eq!(header, vec!["pessoa", "idade_ajustada", "age * 2"]);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].values,
            vec![Value::String("bia".into()), Value::Int(21), Value::Int(40)]
        );
    }
}
