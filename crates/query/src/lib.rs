use query_sheets_core::{DataSource, Row, Schema, Value};
use sqlparser::ast::{
    BinaryOperator, Expr, JoinConstraint, JoinOperator, Query, Select, SelectItem, SetExpr,
    TableFactor, Value as SqlValue,
};
use std::collections::{HashMap, HashSet};

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
use expr::{eval_predicate, eval_value, resolve_column, resolve_compound_column};
use ordering::{
    apply_order_by_to_execution, execute_select_with_order_by,
    order_projected_rows_with_source_fallback,
};
use projection::{build_projection, project_row};
use text::normalize_text_case_insensitive;

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
    execute_with_string_mode_and_resolver_dyn(
        source,
        query,
        string_comparison_mode,
        &mut table_resolver,
    )
}

fn execute_with_string_mode_and_resolver_dyn<'a>(
    source: &'a dyn DataSource,
    query: &str,
    string_comparison_mode: StringComparisonMode,
    table_resolver: &mut dyn FnMut(&TableReference) -> Result<ResolvedTableData, QueryError>,
) -> Result<QueryExecution<'a>, QueryError> {
    let parsed_query = parser::parse_select(query)?;

    if parsed_query.select.from.len() > 1 {
        return Err(QueryError::UnsupportedQuery);
    }

    if query_uses_join(&parsed_query.select) {
        let joined_source = build_joined_source(
            source,
            &parsed_query.select,
            string_comparison_mode,
            table_resolver,
        )?;
        let execution = execute_parsed_select(
            &joined_source,
            &parsed_query,
            string_comparison_mode,
            table_resolver,
        )?;
        let rows = execution.rows.collect::<Vec<_>>();

        return Ok(QueryExecution {
            schema: execution.schema,
            rows: Box::new(rows.into_iter()),
        });
    }

    execute_parsed_select(
        source,
        &parsed_query,
        string_comparison_mode,
        table_resolver,
    )
}

fn execute_parsed_select<'a>(
    source: &'a dyn DataSource,
    parsed_query: &parser::ParsedSelect,
    string_comparison_mode: StringComparisonMode,
    table_resolver: &mut dyn FnMut(&TableReference) -> Result<ResolvedTableData, QueryError>,
) -> Result<QueryExecution<'a>, QueryError> {
    let schema = source.schema().clone();
    let filtered_source = filter_source_with_where(
        source,
        &schema,
        parsed_query.select.selection.as_ref(),
        string_comparison_mode,
        table_resolver,
    )?;
    let effective_source: &dyn DataSource = filtered_source
        .as_ref()
        .map(|value| value as &dyn DataSource)
        .unwrap_or(source);

    if let Some(group_by_columns) =
        extract_group_by_column_indexes(&schema, &parsed_query.select.group_by)?
    {
        let plan =
            build_group_by_aggregation_plan(&schema, &parsed_query.select.projection, &group_by_columns)?;
        let mut execution = execute_group_by_aggregation(
            effective_source,
            &schema,
            None,
            plan,
            string_comparison_mode,
        )?;
        execution = apply_order_by_to_execution(execution, &parsed_query.order_by, string_comparison_mode)?;
        execution = apply_pagination_to_execution(execution, parsed_query.pagination);
        let rows = execution.rows.collect::<Vec<_>>();
        return Ok(QueryExecution {
            schema: execution.schema,
            rows: Box::new(rows.into_iter()),
        });
    }

    let (projection, projected_schema) = build_projection(&schema, &parsed_query.select.projection)?;
    let projection_has_scalar_subquery = projection_uses_scalar_subquery(&parsed_query.select.projection);

    if projection_has_scalar_subquery {
        let mut execution = execute_select_with_scalar_subqueries(
            effective_source,
            source,
            &schema,
            None,
            &projection,
            projected_schema,
            &parsed_query.order_by,
            string_comparison_mode,
            table_resolver,
        )?;
        execution = apply_pagination_to_execution(execution, parsed_query.pagination);
        let rows = execution.rows.collect::<Vec<_>>();
        return Ok(QueryExecution {
            schema: execution.schema,
            rows: Box::new(rows.into_iter()),
        });
    }

    let mut execution = if parsed_query.order_by.is_empty() {
        let iter = effective_source.scan().map(move |row| {
            let values = project_row(&projection, &row, &schema);

            Row::new(values)
        });

        QueryExecution {
            schema: projected_schema,
            rows: Box::new(iter),
        }
    } else {
        execute_select_with_order_by(
            effective_source,
            &schema,
            None,
            &projection,
            projected_schema,
            &parsed_query.order_by,
            string_comparison_mode,
        )?
    };

    execution = apply_pagination_to_execution(execution, parsed_query.pagination);

    let rows = execution.rows.collect::<Vec<_>>();
    Ok(QueryExecution {
        schema: execution.schema,
        rows: Box::new(rows.into_iter()),
    })
}

fn filter_source_with_where(
    source: &dyn DataSource,
    source_schema: &Schema,
    where_expr: Option<&Expr>,
    string_comparison_mode: StringComparisonMode,
    table_resolver: &mut dyn FnMut(&TableReference) -> Result<ResolvedTableData, QueryError>,
) -> Result<Option<InMemoryDataSource>, QueryError> {
    let Some(where_expr) = where_expr else {
        return Ok(None);
    };

    // Materialize non-correlated subqueries once to avoid re-running them for each row.
    let pre_rewritten_where = if expr_contains_subquery(where_expr) {
        Some(rewrite_expr_subqueries(
            where_expr,
            source,
            source_schema,
            string_comparison_mode,
            table_resolver,
            None,
        )?)
    } else {
        None
    };
    let has_remaining_subqueries = pre_rewritten_where
        .as_ref()
        .map(expr_contains_subquery)
        .unwrap_or(false);

    let mut filtered_rows = Vec::new();

    for row in source.scan() {
        let keep = if let Some(rewritten_where) = pre_rewritten_where.as_ref() {
            if has_remaining_subqueries {
                evaluate_where_expr_for_row(
                    rewritten_where,
                    &row,
                    source,
                    source_schema,
                    string_comparison_mode,
                    table_resolver,
                )?
            } else {
                eval_predicate(rewritten_where, &row, source_schema, string_comparison_mode)?
            }
        } else {
            eval_predicate(where_expr, &row, source_schema, string_comparison_mode)?
        };

        if keep {
            filtered_rows.push(row);
        }
    }

    Ok(Some(InMemoryDataSource {
        schema: source_schema.clone(),
        rows: filtered_rows,
    }))
}

fn evaluate_where_expr_for_row(
    where_expr: &Expr,
    row: &Row,
    source: &dyn DataSource,
    source_schema: &Schema,
    string_comparison_mode: StringComparisonMode,
    table_resolver: &mut dyn FnMut(&TableReference) -> Result<ResolvedTableData, QueryError>,
) -> Result<bool, QueryError> {
    if expr_contains_subquery(where_expr) {
        let rewritten = rewrite_expr_subqueries(
            where_expr,
            source,
            source_schema,
            string_comparison_mode,
            table_resolver,
            Some((row, source_schema)),
        )?;

        return eval_predicate(&rewritten, row, source_schema, string_comparison_mode);
    }

    eval_predicate(where_expr, row, source_schema, string_comparison_mode)
}

fn execute_select_with_scalar_subqueries<'a>(
    scan_source: &'a dyn DataSource,
    subquery_source: &dyn DataSource,
    source_schema: &Schema,
    where_expr: Option<&Expr>,
    projection: &[projection::ProjectionItem],
    projected_schema: Schema,
    order_by: &[sqlparser::ast::OrderByExpr],
    string_comparison_mode: StringComparisonMode,
    table_resolver: &mut dyn FnMut(&TableReference) -> Result<ResolvedTableData, QueryError>,
) -> Result<QueryExecution<'a>, QueryError> {
    let mut projected_with_source_rows = Vec::new();

    for source_row in scan_source.scan() {
        if let Some(expr) = where_expr {
            let keep = eval_predicate(expr, &source_row, source_schema, string_comparison_mode)?;
            if !keep {
                continue;
            }
        }

        let values = evaluate_projection_with_scalar_subqueries(
            projection,
            &source_row,
            source_schema,
            subquery_source,
            string_comparison_mode,
            table_resolver,
        )?;

        projected_with_source_rows.push((Row::new(values), source_row));
    }

    let rows = order_projected_rows_with_source_fallback(
        projected_with_source_rows,
        &projected_schema,
        source_schema,
        order_by,
        string_comparison_mode,
    )?;

    Ok(QueryExecution {
        schema: projected_schema,
        rows: Box::new(rows.into_iter()),
    })
}

fn evaluate_projection_with_scalar_subqueries(
    projection: &[projection::ProjectionItem],
    source_row: &Row,
    source_schema: &Schema,
    source: &dyn DataSource,
    string_comparison_mode: StringComparisonMode,
    table_resolver: &mut dyn FnMut(&TableReference) -> Result<ResolvedTableData, QueryError>,
) -> Result<Vec<Value>, QueryError> {
    let mut values = Vec::new();

    for item in projection {
        match item {
            projection::ProjectionItem::Wildcard => {
                values.extend(source_row.values.iter().cloned());
            }
            projection::ProjectionItem::Expr(expr) => {
                let rewritten = rewrite_scalar_subqueries_for_projection(
                    expr,
                    source,
                    source_row,
                    source_schema,
                    string_comparison_mode,
                    table_resolver,
                )?;
                let value = eval_value(&rewritten, source_row, source_schema)
                    .map_err(|_| QueryError::UnsupportedSelect(expr.to_string()))?;
                values.push(value);
            }
        }
    }

    Ok(values)
}

fn rewrite_scalar_subqueries_for_projection(
    expr: &Expr,
    source: &dyn DataSource,
    outer_row: &Row,
    outer_schema: &Schema,
    string_comparison_mode: StringComparisonMode,
    table_resolver: &mut dyn FnMut(&TableReference) -> Result<ResolvedTableData, QueryError>,
) -> Result<Expr, QueryError> {
    match expr {
        Expr::Subquery(subquery) => {
            let value = execute_scalar_subquery_value(
                subquery,
                source,
                string_comparison_mode,
                table_resolver,
                outer_row,
                outer_schema,
            )?;
            Ok(value_to_sql_expr(value))
        }
        Expr::BinaryOp { left, op, right } => {
            let rewritten_left = rewrite_scalar_subqueries_for_projection(
                left,
                source,
                outer_row,
                outer_schema,
                string_comparison_mode,
                table_resolver,
            )?;
            let rewritten_right = rewrite_scalar_subqueries_for_projection(
                right,
                source,
                outer_row,
                outer_schema,
                string_comparison_mode,
                table_resolver,
            )?;

            Ok(Expr::BinaryOp {
                left: Box::new(rewritten_left),
                op: op.clone(),
                right: Box::new(rewritten_right),
            })
        }
        Expr::Nested(inner) => Ok(Expr::Nested(Box::new(
            rewrite_scalar_subqueries_for_projection(
                inner,
                source,
                outer_row,
                outer_schema,
                string_comparison_mode,
                table_resolver,
            )?,
        ))),
        Expr::UnaryOp { op, expr: inner } => Ok(Expr::UnaryOp {
            op: op.clone(),
            expr: Box::new(rewrite_scalar_subqueries_for_projection(
                inner,
                source,
                outer_row,
                outer_schema,
                string_comparison_mode,
                table_resolver,
            )?),
        }),
        Expr::Cast {
            kind,
            expr: inner,
            data_type,
            format,
        } => Ok(Expr::Cast {
            kind: kind.clone(),
            expr: Box::new(rewrite_scalar_subqueries_for_projection(
                inner,
                source,
                outer_row,
                outer_schema,
                string_comparison_mode,
                table_resolver,
            )?),
            data_type: data_type.clone(),
            format: format.clone(),
        }),
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => {
            let rewritten_inner = rewrite_scalar_subqueries_for_projection(
                inner,
                source,
                outer_row,
                outer_schema,
                string_comparison_mode,
                table_resolver,
            )?;
            let rewritten_list = list
                .iter()
                .map(|item| {
                    rewrite_scalar_subqueries_for_projection(
                        item,
                        source,
                        outer_row,
                        outer_schema,
                        string_comparison_mode,
                        table_resolver,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(Expr::InList {
                expr: Box::new(rewritten_inner),
                list: rewritten_list,
                negated: *negated,
            })
        }
        Expr::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => {
            let rewritten_inner = rewrite_scalar_subqueries_for_projection(
                inner,
                source,
                outer_row,
                outer_schema,
                string_comparison_mode,
                table_resolver,
            )?;
            let values = execute_subquery_values(
                subquery,
                source,
                string_comparison_mode,
                table_resolver,
                Some((outer_row, outer_schema)),
            )?;
            let list = values.into_iter().map(value_to_sql_expr).collect::<Vec<_>>();

            Ok(Expr::InList {
                expr: Box::new(rewritten_inner),
                list,
                negated: *negated,
            })
        }
        Expr::Exists { subquery, negated } => {
            let exists = execute_exists_subquery(
                subquery,
                source,
                string_comparison_mode,
                table_resolver,
                *negated,
                Some((outer_row, outer_schema)),
            )?;

            Ok(Expr::Value(SqlValue::Boolean(exists)))
        }
        _ => Ok(expr.clone()),
    }
}

fn projection_uses_scalar_subquery(select_items: &[SelectItem]) -> bool {
    select_items.iter().any(|item| match item {
        SelectItem::UnnamedExpr(expr) => expr_contains_subquery(expr),
        SelectItem::ExprWithAlias { expr, .. } => expr_contains_subquery(expr),
        _ => false,
    })
}

fn expr_contains_subquery(expr: &Expr) -> bool {
    match expr {
        Expr::Subquery(_) => true,
        Expr::BinaryOp { left, right, .. } => {
            expr_contains_subquery(left) || expr_contains_subquery(right)
        }
        Expr::Nested(inner) => expr_contains_subquery(inner),
        Expr::UnaryOp { expr: inner, .. } => expr_contains_subquery(inner),
        Expr::Cast { expr: inner, .. } => expr_contains_subquery(inner),
        Expr::InList { expr: inner, list, .. } => {
            expr_contains_subquery(inner)
                || list.iter().any(expr_contains_subquery)
        }
        Expr::InSubquery { .. } => true,
        Expr::Exists { .. } => true,
        _ => false,
    }
}

fn rewrite_expr_subqueries(
    expr: &Expr,
    source: &dyn DataSource,
    source_schema: &Schema,
    string_comparison_mode: StringComparisonMode,
    table_resolver: &mut dyn FnMut(&TableReference) -> Result<ResolvedTableData, QueryError>,
    outer_context: Option<(&Row, &Schema)>,
) -> Result<Expr, QueryError> {
    match expr {
        Expr::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => {
            let rewritten_inner = rewrite_expr_subqueries(
                inner,
                source,
                source_schema,
                string_comparison_mode,
                table_resolver,
                outer_context,
            )?;

            if outer_context.is_none() && query_references_outer_schema(subquery, source_schema) {
                return Ok(Expr::InSubquery {
                    expr: Box::new(rewritten_inner),
                    subquery: subquery.clone(),
                    negated: *negated,
                });
            }

            let values = execute_subquery_values(
                subquery,
                source,
                string_comparison_mode,
                table_resolver,
                outer_context,
            )?;
            let list = values.into_iter().map(value_to_sql_expr).collect::<Vec<_>>();

            Ok(Expr::InList {
                expr: Box::new(rewritten_inner),
                list,
                negated: *negated,
            })
        }
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => {
            let rewritten_inner = rewrite_expr_subqueries(
                inner,
                source,
                source_schema,
                string_comparison_mode,
                table_resolver,
                outer_context,
            )?;
            let rewritten_list = list
                .iter()
                .map(|item| {
                    rewrite_expr_subqueries(
                        item,
                        source,
                        source_schema,
                        string_comparison_mode,
                        table_resolver,
                        outer_context,
                    )
                })
                .collect::<Result<Vec<_>, _>>()?;

            Ok(Expr::InList {
                expr: Box::new(rewritten_inner),
                list: rewritten_list,
                negated: *negated,
            })
        }
        Expr::BinaryOp { left, op, right } => {
            let rewritten_left =
                rewrite_expr_subqueries(
                    left,
                    source,
                    source_schema,
                    string_comparison_mode,
                    table_resolver,
                    outer_context,
                )?;
            let rewritten_right =
                rewrite_expr_subqueries(
                    right,
                    source,
                    source_schema,
                    string_comparison_mode,
                    table_resolver,
                    outer_context,
                )?;

            Ok(Expr::BinaryOp {
                left: Box::new(rewritten_left),
                op: op.clone(),
                right: Box::new(rewritten_right),
            })
        }
        Expr::Exists { subquery, negated } => {
            if outer_context.is_none() && query_references_outer_schema(subquery, source_schema) {
                return Ok(Expr::Exists {
                    subquery: subquery.clone(),
                    negated: *negated,
                });
            }

            let exists = execute_exists_subquery(
                subquery,
                source,
                string_comparison_mode,
                table_resolver,
                *negated,
                outer_context,
            )?;

            Ok(Expr::Value(SqlValue::Boolean(exists)))
        }
        Expr::Nested(inner) => Ok(Expr::Nested(Box::new(rewrite_expr_subqueries(
            inner,
            source,
            source_schema,
            string_comparison_mode,
            table_resolver,
            outer_context,
        )?))),
        _ => Ok(expr.clone()),
    }
}

fn query_references_outer_schema(query: &Query, outer_schema: &Schema) -> bool {
    let SetExpr::Select(select) = query.body.as_ref() else {
        return false;
    };

    let local_aliases = select_local_aliases(select);

    if select
        .projection
        .iter()
        .any(|item| select_item_references_outer_schema(item, outer_schema, &local_aliases))
    {
        return true;
    }

    if select
        .selection
        .as_ref()
        .map(|expr| expr_references_outer_schema(expr, outer_schema, &local_aliases))
        .unwrap_or(false)
    {
        return true;
    }

    if let Some(order_by) = query.order_by.as_ref() {
        if order_by
            .exprs
            .iter()
            .any(|item| expr_references_outer_schema(&item.expr, outer_schema, &local_aliases))
        {
            return true;
        }
    }

    false
}

fn select_item_references_outer_schema(
    item: &SelectItem,
    outer_schema: &Schema,
    local_aliases: &HashSet<String>,
) -> bool {
    match item {
        SelectItem::UnnamedExpr(expr) => {
            expr_references_outer_schema(expr, outer_schema, local_aliases)
        }
        SelectItem::ExprWithAlias { expr, .. } => {
            expr_references_outer_schema(expr, outer_schema, local_aliases)
        }
        _ => false,
    }
}

fn expr_references_outer_schema(
    expr: &Expr,
    outer_schema: &Schema,
    local_aliases: &HashSet<String>,
) -> bool {
    match expr {
        Expr::CompoundIdentifier(identifiers) => {
            if identifiers.len() < 2 {
                return false;
            }

            let qualifier = identifiers[identifiers.len() - 2].value.to_ascii_lowercase();
            if local_aliases.contains(&qualifier) {
                return false;
            }

            resolve_compound_column(outer_schema, identifiers).is_ok()
        }
        Expr::Subquery(subquery) => query_references_outer_schema(subquery, outer_schema),
        Expr::InSubquery { expr: inner, subquery, .. } => {
            expr_references_outer_schema(inner, outer_schema, local_aliases)
                || query_references_outer_schema(subquery, outer_schema)
        }
        Expr::Exists { subquery, .. } => query_references_outer_schema(subquery, outer_schema),
        Expr::InList { expr: inner, list, .. } => {
            expr_references_outer_schema(inner, outer_schema, local_aliases)
                || list
                    .iter()
                    .any(|item| expr_references_outer_schema(item, outer_schema, local_aliases))
        }
        Expr::BinaryOp { left, right, .. } => {
            expr_references_outer_schema(left, outer_schema, local_aliases)
                || expr_references_outer_schema(right, outer_schema, local_aliases)
        }
        Expr::Nested(inner) => expr_references_outer_schema(inner, outer_schema, local_aliases),
        Expr::UnaryOp { expr: inner, .. } => {
            expr_references_outer_schema(inner, outer_schema, local_aliases)
        }
        Expr::Cast { expr: inner, .. } => {
            expr_references_outer_schema(inner, outer_schema, local_aliases)
        }
        _ => false,
    }
}

fn execute_exists_subquery(
    subquery: &sqlparser::ast::Query,
    source: &dyn DataSource,
    string_comparison_mode: StringComparisonMode,
    table_resolver: &mut dyn FnMut(&TableReference) -> Result<ResolvedTableData, QueryError>,
    negated: bool,
    outer_context: Option<(&Row, &Schema)>,
) -> Result<bool, QueryError> {
    let (_schema, rows) = execute_subquery_rows(
        subquery,
        source,
        string_comparison_mode,
        table_resolver,
        outer_context,
    )?;

    let exists = !rows.is_empty();
    Ok(if negated { !exists } else { exists })
}

fn execute_subquery_values(
    subquery: &sqlparser::ast::Query,
    source: &dyn DataSource,
    string_comparison_mode: StringComparisonMode,
    table_resolver: &mut dyn FnMut(&TableReference) -> Result<ResolvedTableData, QueryError>,
    outer_context: Option<(&Row, &Schema)>,
) -> Result<Vec<Value>, QueryError> {
    let (schema, rows) = execute_subquery_rows(
        subquery,
        source,
        string_comparison_mode,
        table_resolver,
        outer_context,
    )?;

    if schema.columns.len() != 1 {
        return Err(QueryError::UnsupportedWhere(
            "subquery in IN/NOT IN must return exactly one column".to_string(),
        ));
    }

    Ok(rows
        .into_iter()
        .map(|row| row.values.into_iter().next().unwrap_or(Value::Null))
        .collect::<Vec<_>>())
}

fn execute_scalar_subquery_value(
    subquery: &sqlparser::ast::Query,
    source: &dyn DataSource,
    string_comparison_mode: StringComparisonMode,
    table_resolver: &mut dyn FnMut(&TableReference) -> Result<ResolvedTableData, QueryError>,
    outer_row: &Row,
    outer_schema: &Schema,
) -> Result<Value, QueryError> {
    let (schema, rows) = execute_subquery_rows(
        subquery,
        source,
        string_comparison_mode,
        table_resolver,
        Some((outer_row, outer_schema)),
    )?;

    if schema.columns.len() != 1 {
        return Err(QueryError::UnsupportedSelect(
            "scalar subquery must return exactly one column".to_string(),
        ));
    }

    match rows.as_slice() {
        [] => Ok(Value::Null),
        [single] => Ok(single.values.first().cloned().unwrap_or(Value::Null)),
        _ => Err(QueryError::UnsupportedSelect(
            "scalar subquery returned more than one row".to_string(),
        )),
    }
}

fn execute_subquery_rows(
    subquery: &sqlparser::ast::Query,
    source: &dyn DataSource,
    string_comparison_mode: StringComparisonMode,
    table_resolver: &mut dyn FnMut(&TableReference) -> Result<ResolvedTableData, QueryError>,
    outer_context: Option<(&Row, &Schema)>,
) -> Result<(Schema, Vec<Row>), QueryError> {
    let rewritten_query = if let Some((outer_row, outer_schema)) = outer_context {
        rewrite_query_with_outer_refs(subquery, outer_row, outer_schema)?
    } else {
        subquery.clone()
    };

    let sql = rewritten_query.to_string();
    let resolved_source = if let Some(table_ref) = parser::extract_table_reference(&sql)? {
        match table_resolver(&table_ref) {
            Ok(data) => Some(InMemoryDataSource {
                schema: data.schema,
                rows: data.rows,
            }),
            Err(err @ QueryError::TableResolution(_)) => {
                if source_schema_is_qualified(source.schema()) {
                    return Err(err);
                }

                None
            }
            Err(err) => return Err(err),
        }
    } else {
        None
    };

    let execution = if let Some(subquery_source) = resolved_source.as_ref() {
        execute_with_string_mode_and_resolver_dyn(
            subquery_source,
            &sql,
            string_comparison_mode,
            table_resolver,
        )?
    } else {
        execute_with_string_mode_and_resolver_dyn(
            source,
            &sql,
            string_comparison_mode,
            table_resolver,
        )?
    };

    let schema = execution.schema;
    let rows = execution.rows.collect::<Vec<_>>();
    Ok((schema, rows))
}

fn source_schema_is_qualified(schema: &Schema) -> bool {
    schema.columns.iter().any(|column| column.name.contains('.'))
}

fn rewrite_query_with_outer_refs(
    query: &Query,
    outer_row: &Row,
    outer_schema: &Schema,
) -> Result<Query, QueryError> {
    let mut rewritten = query.clone();

    let SetExpr::Select(select) = rewritten.body.as_mut() else {
        return Ok(rewritten);
    };

    let local_aliases = select_local_aliases(select);

    if let Some(selection) = &select.selection {
        select.selection = Some(rewrite_expr_with_outer_refs(
            selection,
            outer_row,
            outer_schema,
            &local_aliases,
        )?);
    }

    for item in &mut select.projection {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                *expr = rewrite_expr_with_outer_refs(expr, outer_row, outer_schema, &local_aliases)?;
            }
            SelectItem::ExprWithAlias { expr, .. } => {
                *expr = rewrite_expr_with_outer_refs(expr, outer_row, outer_schema, &local_aliases)?;
            }
            _ => {}
        }
    }

    if let Some(order_by) = rewritten.order_by.as_mut() {
        for order_item in &mut order_by.exprs {
            order_item.expr = rewrite_expr_with_outer_refs(
                &order_item.expr,
                outer_row,
                outer_schema,
                &local_aliases,
            )?;
        }
    }

    Ok(rewritten)
}

fn select_local_aliases(select: &Select) -> HashSet<String> {
    let mut aliases = HashSet::new();

    if let Some(from) = select.from.first() {
        register_alias_for_table_factor(&from.relation, &mut aliases);
        for join in &from.joins {
            register_alias_for_table_factor(&join.relation, &mut aliases);
        }
    }

    aliases
}

fn register_alias_for_table_factor(relation: &TableFactor, aliases: &mut HashSet<String>) {
    if let TableFactor::Table { name, alias, .. } = relation {
        if let Some(alias_name) = alias.as_ref().map(|value| value.name.value.to_ascii_lowercase()) {
            let _ = aliases.insert(alias_name);
            return;
        }

        if let Some(last) = name.0.last() {
            let _ = aliases.insert(last.value.to_ascii_lowercase());
        }
    }
}

fn rewrite_expr_with_outer_refs(
    expr: &Expr,
    outer_row: &Row,
    outer_schema: &Schema,
    local_aliases: &HashSet<String>,
) -> Result<Expr, QueryError> {
    match expr {
        Expr::CompoundIdentifier(identifiers) => {
            if identifiers.len() >= 2 {
                let qualifier = identifiers[identifiers.len() - 2].value.to_ascii_lowercase();
                if !local_aliases.contains(&qualifier) {
                    if let Ok(index) = resolve_compound_column(outer_schema, identifiers) {
                        let value = outer_row.values.get(index).cloned().unwrap_or(Value::Null);
                        return Ok(value_to_sql_expr(value));
                    }
                }
            }

            Ok(expr.clone())
        }
        Expr::BinaryOp { left, op, right } => Ok(Expr::BinaryOp {
            left: Box::new(rewrite_expr_with_outer_refs(
                left,
                outer_row,
                outer_schema,
                local_aliases,
            )?),
            op: op.clone(),
            right: Box::new(rewrite_expr_with_outer_refs(
                right,
                outer_row,
                outer_schema,
                local_aliases,
            )?),
        }),
        Expr::Nested(inner) => Ok(Expr::Nested(Box::new(rewrite_expr_with_outer_refs(
            inner,
            outer_row,
            outer_schema,
            local_aliases,
        )?))),
        Expr::UnaryOp { op, expr: inner } => Ok(Expr::UnaryOp {
            op: op.clone(),
            expr: Box::new(rewrite_expr_with_outer_refs(
                inner,
                outer_row,
                outer_schema,
                local_aliases,
            )?),
        }),
        Expr::Cast {
            kind,
            expr: inner,
            data_type,
            format,
        } => Ok(Expr::Cast {
            kind: kind.clone(),
            expr: Box::new(rewrite_expr_with_outer_refs(
                inner,
                outer_row,
                outer_schema,
                local_aliases,
            )?),
            data_type: data_type.clone(),
            format: format.clone(),
        }),
        Expr::InList {
            expr: inner,
            list,
            negated,
        } => {
            let rewritten_inner = rewrite_expr_with_outer_refs(
                inner,
                outer_row,
                outer_schema,
                local_aliases,
            )?;
            let rewritten_list = list
                .iter()
                .map(|item| rewrite_expr_with_outer_refs(item, outer_row, outer_schema, local_aliases))
                .collect::<Result<Vec<_>, _>>()?;

            Ok(Expr::InList {
                expr: Box::new(rewritten_inner),
                list: rewritten_list,
                negated: *negated,
            })
        }
        Expr::InSubquery {
            expr: inner,
            subquery,
            negated,
        } => Ok(Expr::InSubquery {
            expr: Box::new(rewrite_expr_with_outer_refs(
                inner,
                outer_row,
                outer_schema,
                local_aliases,
            )?),
            subquery: Box::new(rewrite_query_with_outer_refs(subquery, outer_row, outer_schema)?),
            negated: *negated,
        }),
        _ => Ok(expr.clone()),
    }
}

fn value_to_sql_expr(value: Value) -> Expr {
    let sql_value = match value {
        Value::Int(v) => SqlValue::Number(v.to_string(), false),
        Value::Float(v) => {
            if v.is_finite() {
                SqlValue::Number(v.to_string(), false)
            } else {
                SqlValue::Null
            }
        }
        Value::String(v) => SqlValue::SingleQuotedString(v),
        Value::Bool(v) => SqlValue::Boolean(v),
        Value::Null => SqlValue::Null,
    };

    Expr::Value(sql_value)
}

fn query_uses_join(select: &Select) -> bool {
    select
        .from
        .first()
        .map(|from| !from.joins.is_empty())
        .unwrap_or(false)
}

fn build_joined_source(
    base_source: &dyn DataSource,
    select: &Select,
    string_comparison_mode: StringComparisonMode,
    table_resolver: &mut dyn FnMut(&TableReference) -> Result<ResolvedTableData, QueryError>,
) -> Result<InMemoryDataSource, QueryError> {
    let from = select.from.first().ok_or(QueryError::MissingFrom)?;
    let pushdown_predicates = extract_alias_pushdown_predicates(select.selection.as_ref());

    let (base_ref, base_alias) = parse_table_factor_reference(&from.relation)?;
    let base_relation = ResolvedTableData {
        schema: base_source.schema().clone(),
        rows: base_source.scan().collect::<Vec<_>>(),
    };

    let mut combined_schema = qualify_schema_columns(&base_relation.schema, &base_alias);
    let mut combined_rows = if let Some(predicates) = pushdown_predicates.get(&base_alias.to_ascii_lowercase()) {
        filter_rows_with_predicates(
            base_relation.rows,
            &combined_schema,
            predicates,
            string_comparison_mode,
        )?
    } else {
        base_relation.rows
    };

    let _ = base_ref;

    for join in &from.joins {
        let (join_ref, join_alias) = parse_table_factor_reference(&join.relation)?;
        let right_relation = table_resolver(&join_ref)?;
        let right_schema = qualify_schema_columns(&right_relation.schema, &join_alias);
        let right_rows = if let Some(predicates) = pushdown_predicates.get(&join_alias.to_ascii_lowercase()) {
            filter_rows_with_predicates(
                right_relation.rows,
                &right_schema,
                predicates,
                string_comparison_mode,
            )?
        } else {
            right_relation.rows
        };
        let supported_join = supported_join_constraint_expr(&join.join_operator)?;

        let merged_schema = Schema::new(
            combined_schema
                .columns
                .iter()
                .cloned()
                .chain(right_schema.columns.iter().cloned())
                .collect::<Vec<_>>(),
        );

        let merged_rows = if let Some(plan) = try_build_hash_join_plan(
            supported_join.constraint_expr,
            &merged_schema,
            combined_schema.columns.len(),
            right_schema.columns.len(),
            &combined_rows,
            &right_rows,
        )? {
            execute_hash_join(
                &combined_rows,
                &right_rows,
                &combined_schema,
                &right_schema,
                plan,
                supported_join.kind,
                string_comparison_mode,
            )?
        } else {
            execute_nested_loop_join(
                &combined_rows,
                &right_rows,
                &combined_schema,
                &right_schema,
                &merged_schema,
                supported_join,
                string_comparison_mode,
            )?
        };

        combined_schema = merged_schema;
        combined_rows = merged_rows;
    }

    Ok(InMemoryDataSource {
        schema: combined_schema,
        rows: combined_rows,
    })
}

fn filter_rows_with_predicates(
    rows: Vec<Row>,
    schema: &Schema,
    predicates: &[Expr],
    string_comparison_mode: StringComparisonMode,
) -> Result<Vec<Row>, QueryError> {
    if predicates.is_empty() {
        return Ok(rows);
    }

    let mut filtered = Vec::with_capacity(rows.len());

    for row in rows {
        let mut keep = true;

        for predicate in predicates {
            if !eval_predicate(predicate, &row, schema, string_comparison_mode)? {
                keep = false;
                break;
            }
        }

        if keep {
            filtered.push(row);
        }
    }

    Ok(filtered)
}

fn extract_alias_pushdown_predicates(selection: Option<&Expr>) -> HashMap<String, Vec<Expr>> {
    let Some(selection) = selection else {
        return HashMap::new();
    };

    let mut predicates_by_alias: HashMap<String, Vec<Expr>> = HashMap::new();
    let mut conjuncts = Vec::new();
    collect_and_conjuncts(selection, &mut conjuncts);

    for conjunct in conjuncts {
        let Some(alias) = pushdown_alias_for_predicate(conjunct) else {
            continue;
        };

        predicates_by_alias
            .entry(alias)
            .or_default()
            .push(conjunct.clone());
    }

    predicates_by_alias
}

fn collect_and_conjuncts<'a>(expr: &'a Expr, out: &mut Vec<&'a Expr>) {
    if let Expr::BinaryOp {
        left,
        op: BinaryOperator::And,
        right,
    } = expr
    {
        collect_and_conjuncts(left, out);
        collect_and_conjuncts(right, out);
        return;
    }

    out.push(expr);
}

fn pushdown_alias_for_predicate(expr: &Expr) -> Option<String> {
    let mut qualifiers = HashSet::new();

    if !collect_predicate_qualifiers(expr, &mut qualifiers) {
        return None;
    }

    if qualifiers.len() != 1 {
        return None;
    }

    qualifiers.into_iter().next()
}

fn collect_predicate_qualifiers(expr: &Expr, qualifiers: &mut HashSet<String>) -> bool {
    match expr {
        Expr::CompoundIdentifier(identifiers) => {
            if identifiers.len() < 2 {
                return false;
            }

            qualifiers.insert(identifiers[identifiers.len() - 2].value.to_ascii_lowercase());
            true
        }
        Expr::Identifier(_) => false,
        Expr::BinaryOp { left, right, .. } => {
            collect_predicate_qualifiers(left, qualifiers)
                && collect_predicate_qualifiers(right, qualifiers)
        }
        Expr::Nested(inner) => collect_predicate_qualifiers(inner, qualifiers),
        Expr::UnaryOp { expr: inner, .. } => collect_predicate_qualifiers(inner, qualifiers),
        Expr::Cast { expr: inner, .. } => collect_predicate_qualifiers(inner, qualifiers),
        Expr::Value(_) => true,
        _ => false,
    }
}

fn execute_nested_loop_join(
    combined_rows: &[Row],
    right_rows: &[Row],
    combined_schema: &Schema,
    right_schema: &Schema,
    merged_schema: &Schema,
    supported_join: SupportedJoin<'_>,
    string_comparison_mode: StringComparisonMode,
) -> Result<Vec<Row>, QueryError> {
    let mut merged_rows = Vec::new();
    let left_null_padding = vec![Value::Null; combined_schema.columns.len()];
    let right_null_padding = vec![Value::Null; right_schema.columns.len()];
    let mut right_matched = vec![false; right_rows.len()];

    for left_row in combined_rows {
        let mut matched = false;

        for (right_idx, right_row) in right_rows.iter().enumerate() {
            let mut values = left_row.values.clone();
            values.extend(right_row.values.iter().cloned());
            let candidate = Row::new(values);

            if eval_predicate(
                supported_join.constraint_expr,
                &candidate,
                merged_schema,
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
        for (right_idx, right_row) in right_rows.iter().enumerate() {
            if right_matched[right_idx] {
                continue;
            }

            let mut values = left_null_padding.clone();
            values.extend(right_row.values.iter().cloned());
            merged_rows.push(Row::new(values));
        }
    }

    Ok(merged_rows)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum HashJoinValueKind {
    Numeric,
    String,
    Bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum HashJoinKey {
    Numeric(u64),
    String(String),
    Bool(bool),
}

#[derive(Debug, Clone, Copy)]
struct HashJoinPlan {
    left_key_index: usize,
    right_key_index: usize,
    key_kind: HashJoinValueKind,
}

fn try_build_hash_join_plan(
    constraint_expr: &Expr,
    merged_schema: &Schema,
    left_column_count: usize,
    right_column_count: usize,
    left_rows: &[Row],
    right_rows: &[Row],
) -> Result<Option<HashJoinPlan>, QueryError> {
    let Some((left_idx, right_idx)) = extract_equi_join_key_indexes(
        constraint_expr,
        merged_schema,
        left_column_count,
        right_column_count,
    )? else {
        return Ok(None);
    };

    let left_kind = analyze_hash_join_column_kind(left_rows, left_idx)?;
    let right_kind = analyze_hash_join_column_kind(right_rows, right_idx)?;

    let key_kind = match (left_kind, right_kind) {
        (Some(kind), Some(other)) if kind == other => kind,
        (Some(kind), None) | (None, Some(kind)) => kind,
        _ => return Ok(None),
    };

    Ok(Some(HashJoinPlan {
        left_key_index: left_idx,
        right_key_index: right_idx,
        key_kind,
    }))
}

fn extract_equi_join_key_indexes(
    constraint_expr: &Expr,
    merged_schema: &Schema,
    left_column_count: usize,
    right_column_count: usize,
) -> Result<Option<(usize, usize)>, QueryError> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = constraint_expr
    else {
        return Ok(None);
    };

    let left_idx = resolve_join_column_index(left, merged_schema)?;
    let right_idx = resolve_join_column_index(right, merged_schema)?;

    let (Some(left_idx), Some(right_idx)) = (left_idx, right_idx) else {
        return Ok(None);
    };

    let right_boundary = left_column_count + right_column_count;
    let left_on_left = left_idx < left_column_count;
    let left_on_right = left_idx >= left_column_count && left_idx < right_boundary;
    let right_on_left = right_idx < left_column_count;
    let right_on_right = right_idx >= left_column_count && right_idx < right_boundary;

    if left_on_left && right_on_right {
        return Ok(Some((left_idx, right_idx - left_column_count)));
    }

    if right_on_left && left_on_right {
        return Ok(Some((right_idx, left_idx - left_column_count)));
    }

    Ok(None)
}

fn resolve_join_column_index(expr: &Expr, schema: &Schema) -> Result<Option<usize>, QueryError> {
    match expr {
        Expr::Identifier(identifier) => Ok(Some(resolve_column(schema, identifier)?)),
        Expr::CompoundIdentifier(identifiers) => {
            Ok(Some(resolve_compound_column(schema, identifiers)?))
        }
        Expr::Nested(inner) => resolve_join_column_index(inner, schema),
        _ => Ok(None),
    }
}

fn analyze_hash_join_column_kind(
    rows: &[Row],
    index: usize,
) -> Result<Option<HashJoinValueKind>, QueryError> {
    let mut detected: Option<HashJoinValueKind> = None;

    for row in rows {
        let value = row.values.get(index).unwrap_or(&Value::Null);
        let candidate = match value {
            Value::Int(_) => HashJoinValueKind::Numeric,
            Value::Float(v) => {
                if v.is_nan() {
                    return Ok(None);
                }

                HashJoinValueKind::Numeric
            }
            Value::String(_) => HashJoinValueKind::String,
            Value::Bool(_) => HashJoinValueKind::Bool,
            Value::Null => continue,
        };

        match detected {
            Some(existing) if existing != candidate => return Ok(None),
            Some(_) => {}
            None => detected = Some(candidate),
        }
    }

    Ok(detected)
}

fn execute_hash_join(
    combined_rows: &[Row],
    right_rows: &[Row],
    combined_schema: &Schema,
    right_schema: &Schema,
    plan: HashJoinPlan,
    join_kind: SupportedJoinKind,
    string_comparison_mode: StringComparisonMode,
) -> Result<Vec<Row>, QueryError> {
    if !matches!(join_kind, SupportedJoinKind::Inner) {
        return execute_hash_join_build_right(
            combined_rows,
            right_rows,
            combined_schema,
            right_schema,
            plan,
            join_kind,
            string_comparison_mode,
        );
    }

    if combined_rows.len() <= right_rows.len() {
        return execute_hash_join_build_left_inner(
            combined_rows,
            right_rows,
            plan,
            string_comparison_mode,
        );
    }

    execute_hash_join_build_right(
        combined_rows,
        right_rows,
        combined_schema,
        right_schema,
        plan,
        join_kind,
        string_comparison_mode,
    )
}

fn execute_hash_join_build_right(
    combined_rows: &[Row],
    right_rows: &[Row],
    combined_schema: &Schema,
    right_schema: &Schema,
    plan: HashJoinPlan,
    join_kind: SupportedJoinKind,
    string_comparison_mode: StringComparisonMode,
) -> Result<Vec<Row>, QueryError> {
    let mut right_index_by_key: HashMap<HashJoinKey, Vec<usize>> = HashMap::new();

    for (idx, right_row) in right_rows.iter().enumerate() {
        let value = right_row.values.get(plan.right_key_index).unwrap_or(&Value::Null);
        let key = to_hash_join_key(value, plan.key_kind, string_comparison_mode)?;
        if let Some(key) = key {
            right_index_by_key.entry(key).or_default().push(idx);
        }
    }

    let mut merged_rows = Vec::new();
    let left_null_padding = vec![Value::Null; combined_schema.columns.len()];
    let right_null_padding = vec![Value::Null; right_schema.columns.len()];
    let mut right_matched = vec![false; right_rows.len()];

    for left_row in combined_rows {
        let value = left_row.values.get(plan.left_key_index).unwrap_or(&Value::Null);
        let key = to_hash_join_key(value, plan.key_kind, string_comparison_mode)?;

        let mut matched = false;
        if let Some(key) = key {
            if let Some(right_indexes) = right_index_by_key.get(&key) {
                for right_idx in right_indexes {
                    let right_row = &right_rows[*right_idx];
                    let mut values = left_row.values.clone();
                    values.extend(right_row.values.iter().cloned());
                    merged_rows.push(Row::new(values));
                    matched = true;
                    right_matched[*right_idx] = true;
                }
            }
        }

        if matches!(join_kind, SupportedJoinKind::LeftOuter) && !matched {
            let mut values = left_row.values.clone();
            values.extend(right_null_padding.iter().cloned());
            merged_rows.push(Row::new(values));
        }
    }

    if matches!(join_kind, SupportedJoinKind::RightOuter) {
        for (right_idx, right_row) in right_rows.iter().enumerate() {
            if right_matched[right_idx] {
                continue;
            }

            let mut values = left_null_padding.clone();
            values.extend(right_row.values.iter().cloned());
            merged_rows.push(Row::new(values));
        }
    }

    Ok(merged_rows)
}

fn execute_hash_join_build_left_inner(
    combined_rows: &[Row],
    right_rows: &[Row],
    plan: HashJoinPlan,
    string_comparison_mode: StringComparisonMode,
) -> Result<Vec<Row>, QueryError> {
    let mut left_index_by_key: HashMap<HashJoinKey, Vec<usize>> = HashMap::new();

    for (idx, left_row) in combined_rows.iter().enumerate() {
        let value = left_row.values.get(plan.left_key_index).unwrap_or(&Value::Null);
        let key = to_hash_join_key(value, plan.key_kind, string_comparison_mode)?;
        if let Some(key) = key {
            left_index_by_key.entry(key).or_default().push(idx);
        }
    }

    let mut merged_rows = Vec::new();

    for right_row in right_rows {
        let value = right_row.values.get(plan.right_key_index).unwrap_or(&Value::Null);
        let key = to_hash_join_key(value, plan.key_kind, string_comparison_mode)?;

        if let Some(key) = key {
            if let Some(left_indexes) = left_index_by_key.get(&key) {
                for left_idx in left_indexes {
                    let left_row = &combined_rows[*left_idx];
                    let mut values = left_row.values.clone();
                    values.extend(right_row.values.iter().cloned());
                    merged_rows.push(Row::new(values));
                }
            }
        }
    }

    Ok(merged_rows)
}

fn to_hash_join_key(
    value: &Value,
    kind: HashJoinValueKind,
    string_comparison_mode: StringComparisonMode,
) -> Result<Option<HashJoinKey>, QueryError> {
    match (kind, value) {
        (HashJoinValueKind::Numeric, Value::Int(v)) => {
            Ok(Some(HashJoinKey::Numeric(normalize_numeric_bits(*v as f64))))
        }
        (HashJoinValueKind::Numeric, Value::Float(v)) => {
            if v.is_nan() {
                return Err(QueryError::InvalidJoinCondition(
                    "hash join key has NaN value".to_string(),
                ));
            }

            Ok(Some(HashJoinKey::Numeric(normalize_numeric_bits(*v))))
        }
        (HashJoinValueKind::Numeric, Value::Null)
        | (HashJoinValueKind::String, Value::Null)
        | (HashJoinValueKind::Bool, Value::Null) => Ok(None),
        (HashJoinValueKind::String, Value::String(v)) => {
            let text = match string_comparison_mode {
                StringComparisonMode::CaseInsensitive => normalize_text_case_insensitive(v),
                StringComparisonMode::CaseSensitive => v.clone(),
            };

            Ok(Some(HashJoinKey::String(text)))
        }
        (HashJoinValueKind::Bool, Value::Bool(v)) => Ok(Some(HashJoinKey::Bool(*v))),
        _ => Err(QueryError::InvalidJoinCondition(
            "hash join key has unsupported or mixed value types".to_string(),
        )),
    }
}

fn normalize_numeric_bits(value: f64) -> u64 {
    let normalized = if value == 0.0 { 0.0 } else { value };
    normalized.to_bits()
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
        JoinOperator::Inner(JoinConstraint::On(expr)) => {
            validate_join_constraint_expr(expr)?;

            Ok(SupportedJoin {
                kind: SupportedJoinKind::Inner,
                constraint_expr: expr,
            })
        }
        JoinOperator::LeftOuter(JoinConstraint::On(expr)) => {
            validate_join_constraint_expr(expr)?;

            Ok(SupportedJoin {
                kind: SupportedJoinKind::LeftOuter,
                constraint_expr: expr,
            })
        }
        JoinOperator::RightOuter(JoinConstraint::On(expr)) => {
            validate_join_constraint_expr(expr)?;

            Ok(SupportedJoin {
                kind: SupportedJoinKind::RightOuter,
                constraint_expr: expr,
            })
        }
        JoinOperator::Inner(_) | JoinOperator::LeftOuter(_) | JoinOperator::RightOuter(_) => {
            Err(QueryError::UnsupportedQuery)
        }
        _ => Err(QueryError::UnsupportedQuery),
    }
}

fn validate_join_constraint_expr(expr: &Expr) -> Result<(), QueryError> {
    let Expr::BinaryOp {
        left,
        op: BinaryOperator::Eq,
        right,
    } = expr
    else {
        return Ok(());
    };

    let left_ref = normalized_column_reference(left);
    let right_ref = normalized_column_reference(right);

    if let (Some(left_ref), Some(right_ref)) = (left_ref, right_ref) {
        if left_ref == right_ref {
            return Err(QueryError::InvalidJoinCondition(format!(
                "reflexive predicate '{expr}' compares the same column on both sides"
            )));
        }
    }

    Ok(())
}

fn normalized_column_reference(expr: &Expr) -> Option<String> {
    match expr {
        Expr::Identifier(identifier) => Some(identifier.value.to_ascii_lowercase()),
        Expr::CompoundIdentifier(identifiers) => Some(
            identifiers
                .iter()
                .map(|ident| ident.value.to_ascii_lowercase())
                .collect::<Vec<_>>()
                .join("."),
        ),
        Expr::Nested(inner) => normalized_column_reference(inner),
        _ => None,
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
