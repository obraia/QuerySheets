use query_sheets_core::{Column, DataSource, Row, Schema, Value};
use sqlparser::ast::{
    BinaryOperator, Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr,
    Ident, Query, Select, SelectItem, SetExpr, Statement, TableFactor, UnaryOperator,
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

#[derive(Debug, Clone)]
enum AggregationSelectItem {
    GroupKey(usize),
    CountRows,
}

#[derive(Debug, Clone)]
struct GroupByCountPlan {
    key_column_indexes: Vec<usize>,
    select_items: Vec<AggregationSelectItem>,
    output_schema: Schema,
}

impl QueryEngine for SqlLikeQueryEngine {
    fn execute_with_schema<'a>(
        &self,
        source: &'a dyn DataSource,
        query: &str,
    ) -> Result<QueryExecution<'a>, QueryError> {
        let parsed_select = parse_select(query)?;
        let schema = source.schema().clone();

        if let Some(group_by_columns) = extract_group_by_column_indexes(&schema, &parsed_select.group_by)? {
            let plan = build_group_by_count_plan(&schema, &parsed_select.projection, &group_by_columns)?;
            return execute_group_by_count(source, &schema, parsed_select.selection.as_ref(), plan);
        }

        let (projection, projected_schema) = build_projection(&schema, &parsed_select.projection)?;
        let where_expr = parsed_select.selection;

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

fn extract_group_by_column_indexes(
    schema: &Schema,
    group_by: &GroupByExpr,
) -> Result<Option<Vec<usize>>, QueryError> {
    match group_by {
        GroupByExpr::All(_) => Err(QueryError::UnsupportedQuery),
        GroupByExpr::Expressions(expressions, modifiers) => {
            if expressions.is_empty() && modifiers.is_empty() {
                return Ok(None);
            }

            if !modifiers.is_empty() {
                return Err(QueryError::UnsupportedQuery);
            }

            let mut indexes = Vec::with_capacity(expressions.len());
            for expr in expressions {
                let index = match expr {
                    Expr::Identifier(identifier) => resolve_column(schema, identifier)?,
                    Expr::CompoundIdentifier(identifiers) => resolve_compound_column(schema, identifiers)?,
                    _ => return Err(QueryError::UnsupportedSelect(expr.to_string())),
                };
                indexes.push(index);
            }

            Ok(Some(indexes))
        }
    }
}

fn build_group_by_count_plan(
    schema: &Schema,
    select_items: &[SelectItem],
    group_by_column_indexes: &[usize],
) -> Result<GroupByCountPlan, QueryError> {
    if select_items.is_empty() {
        return Err(QueryError::UnsupportedSelect("projection is empty".to_string()));
    }

    let mut plan_items = Vec::with_capacity(select_items.len());
    let mut output_columns = Vec::with_capacity(select_items.len());
    let mut has_count = false;

    for item in select_items {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let plan_item = parse_group_select_expr(schema, expr, group_by_column_indexes)?;
                if matches!(plan_item, AggregationSelectItem::CountRows) {
                    has_count = true;
                }

                plan_items.push(plan_item);
                output_columns.push(Column::new(projection_output_name(expr)));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let plan_item = parse_group_select_expr(schema, expr, group_by_column_indexes)?;
                if matches!(plan_item, AggregationSelectItem::CountRows) {
                    has_count = true;
                }

                plan_items.push(plan_item);
                output_columns.push(Column::new(alias.value.clone()));
            }
            other => {
                return Err(QueryError::UnsupportedSelect(other.to_string()));
            }
        }
    }

    if !has_count {
        return Err(QueryError::UnsupportedSelect(
            "GROUP BY queries currently require COUNT(*) in projection".to_string(),
        ));
    }

    Ok(GroupByCountPlan {
        key_column_indexes: group_by_column_indexes.to_vec(),
        select_items: plan_items,
        output_schema: Schema::new(output_columns),
    })
}

fn parse_group_select_expr(
    schema: &Schema,
    expr: &Expr,
    group_by_column_indexes: &[usize],
) -> Result<AggregationSelectItem, QueryError> {
    match expr {
        Expr::Identifier(identifier) => {
            let column_index = resolve_column(schema, identifier)?;
            let key_index = group_by_column_indexes
                .iter()
                .position(|idx| *idx == column_index)
                .ok_or_else(|| QueryError::UnsupportedSelect(expr.to_string()))?;
            Ok(AggregationSelectItem::GroupKey(key_index))
        }
        Expr::CompoundIdentifier(identifiers) => {
            let column_index = resolve_compound_column(schema, identifiers)?;
            let key_index = group_by_column_indexes
                .iter()
                .position(|idx| *idx == column_index)
                .ok_or_else(|| QueryError::UnsupportedSelect(expr.to_string()))?;
            Ok(AggregationSelectItem::GroupKey(key_index))
        }
        Expr::Function(function) => {
            if is_count_star(function) {
                return Ok(AggregationSelectItem::CountRows);
            }

            Err(QueryError::UnsupportedSelect(expr.to_string()))
        }
        _ => Err(QueryError::UnsupportedSelect(expr.to_string())),
    }
}

fn is_count_star(function: &Function) -> bool {
    let Some(function_name) = function.name.0.last() else {
        return false;
    };

    if !function_name.value.eq_ignore_ascii_case("count") {
        return false;
    }

    if function.filter.is_some() || function.over.is_some() || !function.within_group.is_empty() {
        return false;
    }

    if !matches!(function.parameters, FunctionArguments::None) {
        return false;
    }

    let FunctionArguments::List(arg_list) = &function.args else {
        return false;
    };

    if arg_list.duplicate_treatment.is_some() || !arg_list.clauses.is_empty() || arg_list.args.len() != 1 {
        return false;
    }

    matches!(
        &arg_list.args[0],
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
    )
}

fn execute_group_by_count<'a>(
    source: &'a dyn DataSource,
    schema: &Schema,
    where_expr: Option<&Expr>,
    plan: GroupByCountPlan,
) -> Result<QueryExecution<'a>, QueryError> {
    let GroupByCountPlan {
        key_column_indexes,
        select_items,
        output_schema,
    } = plan;

    let mut groups: Vec<(Vec<Value>, i64)> = Vec::new();

    for row in source.scan() {
        if let Some(expr) = where_expr {
            let keep = eval_predicate(expr, &row, schema).unwrap_or(false);
            if !keep {
                continue;
            }
        }

        let key_values = key_column_indexes
            .iter()
            .map(|column_index| row.values.get(*column_index).cloned().unwrap_or(Value::Null))
            .collect::<Vec<_>>();

        if let Some((_, count)) = groups.iter_mut().find(|(key, _)| key == &key_values) {
            *count += 1;
        } else {
            groups.push((key_values, 1));
        }
    }

    let rows = groups.into_iter().map(move |(key_values, count)| {
        let values = select_items
            .iter()
            .map(|item| match item {
                AggregationSelectItem::GroupKey(key_index) => {
                    key_values.get(*key_index).cloned().unwrap_or(Value::Null)
                }
                AggregationSelectItem::CountRows => Value::Int(count),
            })
            .collect::<Vec<_>>();

        Row::new(values)
    });

    Ok(QueryExecution {
        schema: output_schema,
        rows: Box::new(rows),
    })
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
    use super::{QueryEngine, QueryError, SqlLikeQueryEngine};
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

    #[test]
    fn executes_group_by_with_count_and_aliases() {
        let source = MockSource {
            schema: Schema::new(vec![Column::new("name"), Column::new("segment")]),
            rows: vec![
                Row::new(vec![Value::String("ana".into()), Value::String("Enterprise".into())]),
                Row::new(vec![Value::String("bia".into()), Value::String("SMB".into())]),
                Row::new(vec![Value::String("caio".into()), Value::String("Enterprise".into())]),
            ],
        };

        let engine = SqlLikeQueryEngine;
        let execution = engine
            .execute_with_schema(
                &source,
                "SELECT segment AS customer_segment, COUNT(*) AS total FROM planilha GROUP BY segment",
            )
            .expect("query should execute");

        let header = execution
            .schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let rows = execution.rows.collect::<Vec<_>>();

        assert_eq!(header, vec!["customer_segment", "total"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].values,
            vec![Value::String("Enterprise".into()), Value::Int(2)]
        );
        assert_eq!(rows[1].values, vec![Value::String("SMB".into()), Value::Int(1)]);
    }

    #[test]
    fn returns_error_when_projection_has_non_grouped_column_in_group_query() {
        let source = MockSource {
            schema: Schema::new(vec![Column::new("name"), Column::new("age")]),
            rows: vec![
                Row::new(vec![Value::String("ana".into()), Value::Int(10)]),
                Row::new(vec![Value::String("bia".into()), Value::Int(20)]),
            ],
        };

        let engine = SqlLikeQueryEngine;
        let err = match engine.execute(
            &source,
            "SELECT name, age, COUNT(*) FROM planilha GROUP BY name",
        ) {
            Ok(_) => panic!("query should fail"),
            Err(err) => err,
        };

        match err {
            QueryError::UnsupportedSelect(message) => assert!(message.contains("age")),
            other => panic!("expected UnsupportedSelect error, got {other:?}"),
        }
    }
}
