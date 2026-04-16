use query_sheets_core::{DataSource, Row, Schema, Value};
use sqlparser::ast::{Expr, JoinConstraint, JoinOperator, Select, TableFactor};

mod aggregation;
mod errors;
mod expr;
mod ordering;
mod parser;
mod projection;
mod text;

pub use errors::QueryError;
pub use parser::{TableReference, extract_table_name, extract_table_reference};

use aggregation::{
    build_group_by_aggregation_plan, execute_group_by_aggregation, extract_group_by_column_indexes,
};
use expr::eval_predicate;
use ordering::{apply_order_by_to_execution, execute_select_with_order_by};
use projection::{build_projection, project_row};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StringComparisonMode {
    CaseInsensitive,
    CaseSensitive,
}

impl StringComparisonMode {
    fn from_case_sensitive(case_sensitive: bool) -> Self {
        if case_sensitive {
            Self::CaseSensitive
        } else {
            Self::CaseInsensitive
        }
    }
}

pub trait QueryEngine {
    fn execute_with_schema<'a>(
        &self,
        source: &'a dyn DataSource,
        query: &str,
    ) -> Result<QueryExecution<'a>, QueryError>;

    fn execute_with_schema_and_resolver<'a, F>(
        &self,
        source: &'a dyn DataSource,
        query: &str,
        _table_resolver: F,
    ) -> Result<QueryExecution<'a>, QueryError>
    where
        F: FnMut(&TableReference) -> Result<ResolvedTableData, QueryError>,
    {
        self.execute_with_schema(source, query)
    }

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

#[derive(Debug, Clone, Copy)]
pub struct ConfiguredSqlLikeQueryEngine {
    string_comparison_mode: StringComparisonMode,
}

impl SqlLikeQueryEngine {
    pub fn with_case_sensitive_strings(
        self,
        case_sensitive_strings: bool,
    ) -> ConfiguredSqlLikeQueryEngine {
        ConfiguredSqlLikeQueryEngine {
            string_comparison_mode: StringComparisonMode::from_case_sensitive(
                case_sensitive_strings,
            ),
        }
    }
}

pub struct QueryExecution<'a> {
    pub schema: Schema,
    pub rows: Box<dyn Iterator<Item = Row> + 'a>,
}

#[derive(Debug, Clone)]
pub struct ResolvedTableData {
    pub schema: Schema,
    pub rows: Vec<Row>,
}

impl QueryEngine for SqlLikeQueryEngine {
    fn execute_with_schema<'a>(
        &self,
        source: &'a dyn DataSource,
        query: &str,
    ) -> Result<QueryExecution<'a>, QueryError> {
        execute_with_string_mode_and_resolver(
            source,
            query,
            StringComparisonMode::CaseInsensitive,
            |table_ref| {
                Err(QueryError::TableResolution(format!(
                    "table '{}' is not available in the current source",
                    table_ref.table
                )))
            },
        )
    }

    fn execute_with_schema_and_resolver<'a, F>(
        &self,
        source: &'a dyn DataSource,
        query: &str,
        table_resolver: F,
    ) -> Result<QueryExecution<'a>, QueryError>
    where
        F: FnMut(&TableReference) -> Result<ResolvedTableData, QueryError>,
    {
        execute_with_string_mode_and_resolver(
            source,
            query,
            StringComparisonMode::CaseInsensitive,
            table_resolver,
        )
    }
}

impl QueryEngine for ConfiguredSqlLikeQueryEngine {
    fn execute_with_schema<'a>(
        &self,
        source: &'a dyn DataSource,
        query: &str,
    ) -> Result<QueryExecution<'a>, QueryError> {
        execute_with_string_mode_and_resolver(
            source,
            query,
            self.string_comparison_mode,
            |table_ref| {
                Err(QueryError::TableResolution(format!(
                    "table '{}' is not available in the current source",
                    table_ref.table
                )))
            },
        )
    }

    fn execute_with_schema_and_resolver<'a, F>(
        &self,
        source: &'a dyn DataSource,
        query: &str,
        table_resolver: F,
    ) -> Result<QueryExecution<'a>, QueryError>
    where
        F: FnMut(&TableReference) -> Result<ResolvedTableData, QueryError>,
    {
        execute_with_string_mode_and_resolver(
            source,
            query,
            self.string_comparison_mode,
            table_resolver,
        )
    }
}

fn execute_with_string_mode_and_resolver<'a, F>(
    source: &'a dyn DataSource,
    query: &str,
    string_comparison_mode: StringComparisonMode,
    mut table_resolver: F,
) -> Result<QueryExecution<'a>, QueryError>
where
    F: FnMut(&TableReference) -> Result<ResolvedTableData, QueryError>,
{
    let parsed_query = parser::parse_select(query)?;

    if parsed_query.select.from.len() > 1 {
        return Err(QueryError::UnsupportedQuery);
    }

    if query_uses_join(&parsed_query.select) {
        let joined_source = build_joined_source(
            source,
            &parsed_query.select,
            string_comparison_mode,
            &mut table_resolver,
        )?;
        let execution = execute_parsed_select(&joined_source, &parsed_query, string_comparison_mode)?;
        let rows = execution.rows.collect::<Vec<_>>();

        return Ok(QueryExecution {
            schema: execution.schema,
            rows: Box::new(rows.into_iter()),
        });
    }

    execute_parsed_select(source, &parsed_query, string_comparison_mode)
}

fn execute_parsed_select<'a>(
    source: &'a dyn DataSource,
    parsed_query: &parser::ParsedSelect,
    string_comparison_mode: StringComparisonMode,
) -> Result<QueryExecution<'a>, QueryError> {
    let schema = source.schema().clone();

    if let Some(group_by_columns) =
        extract_group_by_column_indexes(&schema, &parsed_query.select.group_by)?
    {
        let plan =
            build_group_by_aggregation_plan(&schema, &parsed_query.select.projection, &group_by_columns)?;
        let mut execution = execute_group_by_aggregation(
            source,
            &schema,
            parsed_query.select.selection.as_ref(),
            plan,
            string_comparison_mode,
        )?;
        execution = apply_order_by_to_execution(execution, &parsed_query.order_by, string_comparison_mode)?;
        execution = apply_pagination_to_execution(execution, parsed_query.pagination);
        return Ok(execution);
    }

    let (projection, projected_schema) = build_projection(&schema, &parsed_query.select.projection)?;
    let where_expr = parsed_query.select.selection.clone();

    let mut execution = if parsed_query.order_by.is_empty() {
        let iter = source.scan().filter_map(move |row| {
            if let Some(expr) = &where_expr {
                let keep = eval_predicate(expr, &row, &schema, string_comparison_mode)
                    .unwrap_or(false);
                if !keep {
                    return None;
                }
            }

            let values = project_row(&projection, &row, &schema);

            Some(Row::new(values))
        });

        QueryExecution {
            schema: projected_schema,
            rows: Box::new(iter),
        }
    } else {
        execute_select_with_order_by(
            source,
            &schema,
            where_expr.as_ref(),
            &projection,
            projected_schema,
            &parsed_query.order_by,
            string_comparison_mode,
        )?
    };

    execution = apply_pagination_to_execution(execution, parsed_query.pagination);

    Ok(execution)
}

fn query_uses_join(select: &Select) -> bool {
    select
        .from
        .first()
        .map(|from| !from.joins.is_empty())
        .unwrap_or(false)
}

fn build_joined_source<F>(
    base_source: &dyn DataSource,
    select: &Select,
    string_comparison_mode: StringComparisonMode,
    table_resolver: &mut F,
) -> Result<InMemoryDataSource, QueryError>
where
    F: FnMut(&TableReference) -> Result<ResolvedTableData, QueryError>,
{
    let from = select.from.first().ok_or(QueryError::MissingFrom)?;

    let (base_ref, base_alias) = parse_table_factor_reference(&from.relation)?;
    let base_relation = ResolvedTableData {
        schema: base_source.schema().clone(),
        rows: base_source.scan().collect::<Vec<_>>(),
    };

    let mut combined_schema = qualify_schema_columns(&base_relation.schema, &base_alias);
    let mut combined_rows = base_relation.rows;

    let _ = base_ref;

    for join in &from.joins {
        let (join_ref, join_alias) = parse_table_factor_reference(&join.relation)?;
        let right_relation = table_resolver(&join_ref)?;
        let right_schema = qualify_schema_columns(&right_relation.schema, &join_alias);
        let supported_join = supported_join_constraint_expr(&join.join_operator)?;

        let merged_schema = Schema::new(
            combined_schema
                .columns
                .iter()
                .cloned()
                .chain(right_schema.columns.iter().cloned())
                .collect::<Vec<_>>(),
        );

        let mut merged_rows = Vec::new();
        let left_null_padding = vec![Value::Null; combined_schema.columns.len()];
        let right_null_padding = vec![Value::Null; right_schema.columns.len()];
        let mut right_matched = vec![false; right_relation.rows.len()];

        for left_row in &combined_rows {
            let mut matched = false;

            for (right_idx, right_row) in right_relation.rows.iter().enumerate() {
                let mut values = left_row.values.clone();
                values.extend(right_row.values.iter().cloned());
                let candidate = Row::new(values);

                if eval_predicate(
                    supported_join.constraint_expr,
                    &candidate,
                    &merged_schema,
                    string_comparison_mode,
                )? {
                    matched = true;
                    right_matched[right_idx] = true;
                    merged_rows.push(candidate);
                }
            }

            if matches!(supported_join.kind, SupportedJoinKind::LeftOuter) && !matched {
                let mut values = left_row.values.clone();
                values.extend(right_null_padding.iter().cloned());
                merged_rows.push(Row::new(values));
            }
        }

        if matches!(supported_join.kind, SupportedJoinKind::RightOuter) {
            for (right_idx, right_row) in right_relation.rows.iter().enumerate() {
                if right_matched[right_idx] {
                    continue;
                }

                let mut values = left_null_padding.clone();
                values.extend(right_row.values.iter().cloned());
                merged_rows.push(Row::new(values));
            }
        }

        combined_schema = merged_schema;
        combined_rows = merged_rows;
    }

    Ok(InMemoryDataSource {
        schema: combined_schema,
        rows: combined_rows,
    })
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SupportedJoinKind {
    Inner,
    LeftOuter,
    RightOuter,
}

#[derive(Debug, Clone, Copy)]
struct SupportedJoin<'a> {
    kind: SupportedJoinKind,
    constraint_expr: &'a Expr,
}

fn supported_join_constraint_expr(join_operator: &JoinOperator) -> Result<SupportedJoin<'_>, QueryError> {
    match join_operator {
        JoinOperator::Inner(JoinConstraint::On(expr)) => Ok(SupportedJoin {
            kind: SupportedJoinKind::Inner,
            constraint_expr: expr,
        }),
        JoinOperator::LeftOuter(JoinConstraint::On(expr)) => Ok(SupportedJoin {
            kind: SupportedJoinKind::LeftOuter,
            constraint_expr: expr,
        }),
        JoinOperator::RightOuter(JoinConstraint::On(expr)) => Ok(SupportedJoin {
            kind: SupportedJoinKind::RightOuter,
            constraint_expr: expr,
        }),
        JoinOperator::Inner(_) | JoinOperator::LeftOuter(_) | JoinOperator::RightOuter(_) => {
            Err(QueryError::UnsupportedQuery)
        }
        _ => Err(QueryError::UnsupportedQuery),
    }
}

fn parse_table_factor_reference(relation: &TableFactor) -> Result<(TableReference, String), QueryError> {
    match relation {
        TableFactor::Table { name, alias, .. } => {
            let Some(table_ident) = name.0.last() else {
                return Err(QueryError::UnsupportedQuery);
            };

            let schema = if name.0.len() >= 2 {
                Some(name.0[name.0.len() - 2].value.clone())
            } else {
                None
            };

            let table_name = table_ident.value.clone();
            let alias_name = alias
                .as_ref()
                .map(|value| value.name.value.clone())
                .unwrap_or_else(|| table_name.clone());

            Ok((
                TableReference {
                    schema,
                    table: table_name,
                },
                alias_name,
            ))
        }
        _ => Err(QueryError::UnsupportedQuery),
    }
}

fn qualify_schema_columns(schema: &Schema, qualifier: &str) -> Schema {
    Schema::new(
        schema
            .columns
            .iter()
            .map(|column| query_sheets_core::Column::new(format!("{qualifier}.{}", column.name)))
            .collect::<Vec<_>>(),
    )
}

#[derive(Debug, Clone)]
struct InMemoryDataSource {
    schema: Schema,
    rows: Vec<Row>,
}

impl DataSource for InMemoryDataSource {
    fn schema(&self) -> &Schema {
        &self.schema
    }

    fn scan<'a>(&'a self) -> Box<dyn Iterator<Item = Row> + 'a> {
        Box::new(self.rows.iter().cloned())
    }
}

fn apply_pagination_to_execution<'a>(
    execution: QueryExecution<'a>,
    pagination: parser::Pagination,
) -> QueryExecution<'a> {
    let QueryExecution { schema, rows } = execution;
    let rows = apply_pagination(rows, pagination);

    QueryExecution { schema, rows }
}

fn apply_pagination<'a>(
    rows: Box<dyn Iterator<Item = Row> + 'a>,
    pagination: parser::Pagination,
) -> Box<dyn Iterator<Item = Row> + 'a> {
    let rows = if pagination.offset > 0 {
        Box::new(rows.skip(pagination.offset)) as Box<dyn Iterator<Item = Row> + 'a>
    } else {
        rows
    };

    if let Some(limit) = pagination.limit {
        Box::new(rows.take(limit))
    } else {
        rows
    }
}

#[cfg(test)]
mod tests;
