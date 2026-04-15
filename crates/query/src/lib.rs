use query_sheets_core::{DataSource, Row, Schema};

mod aggregation;
mod errors;
mod expr;
mod ordering;
mod parser;
mod projection;
mod text;

pub use errors::QueryError;
pub use parser::extract_table_name;

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

impl QueryEngine for SqlLikeQueryEngine {
    fn execute_with_schema<'a>(
        &self,
        source: &'a dyn DataSource,
        query: &str,
    ) -> Result<QueryExecution<'a>, QueryError> {
        execute_with_string_mode(source, query, StringComparisonMode::CaseInsensitive)
    }
}

impl QueryEngine for ConfiguredSqlLikeQueryEngine {
    fn execute_with_schema<'a>(
        &self,
        source: &'a dyn DataSource,
        query: &str,
    ) -> Result<QueryExecution<'a>, QueryError> {
        execute_with_string_mode(source, query, self.string_comparison_mode)
    }
}

fn execute_with_string_mode<'a>(
    source: &'a dyn DataSource,
    query: &str,
    string_comparison_mode: StringComparisonMode,
) -> Result<QueryExecution<'a>, QueryError> {
    let parsed_query = parser::parse_select(query)?;
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
    let where_expr = parsed_query.select.selection;

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
