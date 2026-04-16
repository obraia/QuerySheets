use crate::expr::{eval_predicate, eval_value};
use crate::projection::{ProjectionItem, project_row};
use crate::text::compare_text_case_insensitive;
use crate::{QueryError, QueryExecution, StringComparisonMode};
#[cfg(feature = "parallel")]
use crate::parallel_execution_enabled;
use query_sheets_core::{DataSource, Row, Schema, Value};
use sqlparser::ast::{Expr, OrderByExpr, UnaryOperator, Value as SqlValue};
use std::cmp::Ordering;

#[cfg(feature = "parallel")]
use rayon::prelude::*;

pub(crate) fn execute_select_with_order_by<'a>(
    source: &'a dyn DataSource,
    source_schema: &Schema,
    where_expr: Option<&Expr>,
    projection: &[ProjectionItem],
    projected_schema: Schema,
    order_by: &[OrderByExpr],
    string_comparison_mode: StringComparisonMode,
) -> Result<QueryExecution<'a>, QueryError> {
    let filtered_rows = source
        .scan()
        .filter(|row| {
            if let Some(expr) = where_expr {
                return eval_predicate(expr, row, source_schema, string_comparison_mode)
                    .unwrap_or(false);
            }

            true
        })
        .collect::<Vec<_>>();

    let mut sortable_rows = filtered_rows
        .into_iter()
        .map(|source_row| {
            build_sortable_projected_row(
                source_row,
                source_schema,
                projection,
                &projected_schema,
                order_by,
            )
        })
        .collect::<Result<Vec<_>, _>>()?;

    validate_order_by_types(order_by, &sortable_rows)?;
    sort_sortable_rows(&mut sortable_rows, order_by, string_comparison_mode);

    let rows = sortable_rows.into_iter().map(|sortable| sortable.row);
    Ok(QueryExecution {
        schema: projected_schema,
        rows: Box::new(rows),
    })
}

pub(crate) fn apply_order_by_to_execution<'a>(
    execution: QueryExecution<'a>,
    order_by: &[OrderByExpr],
    string_comparison_mode: StringComparisonMode,
) -> Result<QueryExecution<'a>, QueryError> {
    if order_by.is_empty() {
        return Ok(execution);
    }

    let QueryExecution { schema, rows } = execution;
    let mut sortable_rows = rows
        .map(|row| build_sortable_row(row, &schema, order_by))
        .collect::<Result<Vec<_>, _>>()?;

    validate_order_by_types(order_by, &sortable_rows)?;
    sort_sortable_rows(&mut sortable_rows, order_by, string_comparison_mode);

    let rows = sortable_rows.into_iter().map(|sortable| sortable.row);
    Ok(QueryExecution {
        schema,
        rows: Box::new(rows),
    })
}

pub(crate) fn order_projected_rows_with_source_fallback(
    projected_with_source_rows: Vec<(Row, Row)>,
    projected_schema: &Schema,
    source_schema: &Schema,
    order_by: &[OrderByExpr],
    string_comparison_mode: StringComparisonMode,
) -> Result<Vec<Row>, QueryError> {
    if order_by.is_empty() {
        return Ok(projected_with_source_rows
            .into_iter()
            .map(|(projected_row, _)| projected_row)
            .collect::<Vec<_>>());
    }

    let mut sortable_rows = projected_with_source_rows
        .into_iter()
        .map(|(projected_row, source_row)| {
            let keys = build_order_by_keys(
                &projected_row,
                projected_schema,
                Some((&source_row, source_schema)),
                order_by,
            )?;

            Ok(SortableRow {
                row: projected_row,
                keys,
            })
        })
        .collect::<Result<Vec<_>, QueryError>>()?;

    validate_order_by_types(order_by, &sortable_rows)?;
    sort_sortable_rows(&mut sortable_rows, order_by, string_comparison_mode);

    Ok(sortable_rows
        .into_iter()
        .map(|sortable| sortable.row)
        .collect::<Vec<_>>())
}

fn build_sortable_row(
    row: Row,
    schema: &Schema,
    order_by: &[OrderByExpr],
) -> Result<SortableRow, QueryError> {
    let keys = build_order_by_keys(&row, schema, None, order_by)?;

    Ok(SortableRow { row, keys })
}

fn sort_sortable_rows(
    sortable_rows: &mut [SortableRow],
    order_by: &[OrderByExpr],
    string_comparison_mode: StringComparisonMode,
) {
    #[cfg(feature = "parallel")]
    {
        const PARALLEL_SORT_THRESHOLD: usize = 4_096;

        if parallel_execution_enabled() && sortable_rows.len() >= PARALLEL_SORT_THRESHOLD {
            sortable_rows.par_sort_by(|left, right| {
                compare_sortable_rows(left, right, order_by, string_comparison_mode)
            });
            return;
        }
    }

    sortable_rows
        .sort_by(|left, right| compare_sortable_rows(left, right, order_by, string_comparison_mode));
}

fn build_sortable_projected_row(
    source_row: Row,
    source_schema: &Schema,
    projection: &[ProjectionItem],
    projected_schema: &Schema,
    order_by: &[OrderByExpr],
) -> Result<SortableRow, QueryError> {
    let projected_row = Row::new(project_row(projection, &source_row, source_schema));
    let keys = build_order_by_keys(
        &projected_row,
        projected_schema,
        Some((&source_row, source_schema)),
        order_by,
    )?;

    Ok(SortableRow {
        row: projected_row,
        keys,
    })
}

fn build_order_by_keys(
    projected_row: &Row,
    projected_schema: &Schema,
    source_fallback: Option<(&Row, &Schema)>,
    order_by: &[OrderByExpr],
) -> Result<Vec<Value>, QueryError> {
    let mut keys = Vec::with_capacity(order_by.len());

    for item in order_by {
        keys.push(resolve_order_by_value(
            item,
            projected_row,
            projected_schema,
            source_fallback,
        )?);
    }

    Ok(keys)
}

fn resolve_order_by_value(
    item: &OrderByExpr,
    projected_row: &Row,
    projected_schema: &Schema,
    source_fallback: Option<(&Row, &Schema)>,
) -> Result<Value, QueryError> {
    if let Some(position) = parse_order_by_position(&item.expr) {
        if position == 0 {
            return Err(QueryError::UnsupportedOrderBy(
                "ORDER BY position must start at 1".to_string(),
            ));
        }

        return projected_row
            .values
            .get(position - 1)
            .cloned()
            .ok_or_else(|| {
                QueryError::UnsupportedOrderBy(format!(
                    "ORDER BY position {position} is out of range"
                ))
            });
    }

    if let Ok(value) = eval_value(&item.expr, projected_row, projected_schema) {
        return Ok(value);
    }

    if let Some((source_row, source_schema)) = source_fallback {
        if let Ok(value) = eval_value(&item.expr, source_row, source_schema) {
            return Ok(value);
        }
    }

    Err(QueryError::UnsupportedOrderBy(item.expr.to_string()))
}

fn parse_order_by_position(expr: &Expr) -> Option<usize> {
    let number = match expr {
        Expr::Value(SqlValue::Number(number, _)) => number.as_str(),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => return parse_order_by_position(expr),
        _ => return None,
    };

    if number.contains('.') {
        return None;
    }

    number.parse::<usize>().ok()
}

fn validate_order_by_types(order_by: &[OrderByExpr], rows: &[SortableRow]) -> Result<(), QueryError> {
    for (idx, item) in order_by.iter().enumerate() {
        let mut observed_type: Option<OrderValueKind> = None;

        for row in rows {
            let Some(kind) = order_value_kind(&row.keys[idx]) else {
                continue;
            };

            if let Some(previous_kind) = observed_type {
                if previous_kind != kind {
                    return Err(QueryError::UnsupportedOrderBy(format!(
                        "{} must produce comparable values",
                        item.expr
                    )));
                }
            } else {
                observed_type = Some(kind);
            }
        }
    }

    Ok(())
}

fn compare_sortable_rows(
    left: &SortableRow,
    right: &SortableRow,
    order_by: &[OrderByExpr],
    string_comparison_mode: StringComparisonMode,
) -> Ordering {
    for (idx, item) in order_by.iter().enumerate() {
        let nulls_first = item.nulls_first.unwrap_or(false);
        let asc = item.asc.unwrap_or(true);

        let ordering = compare_order_values(
            &left.keys[idx],
            &right.keys[idx],
            asc,
            nulls_first,
            string_comparison_mode,
        );

        if ordering != Ordering::Equal {
            return ordering;
        }
    }

    Ordering::Equal
}

fn compare_order_values(
    left: &Value,
    right: &Value,
    asc: bool,
    nulls_first: bool,
    string_comparison_mode: StringComparisonMode,
) -> Ordering {
    match (left, right) {
        (Value::Null, Value::Null) => Ordering::Equal,
        (Value::Null, _) => {
            if nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (_, Value::Null) => {
            if nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (Value::Int(a), Value::Int(b)) => apply_sort_direction(a.cmp(b), asc),
        (Value::Int(a), Value::Float(b)) => {
            apply_sort_direction((*a as f64).partial_cmp(b).unwrap_or(Ordering::Equal), asc)
        }
        (Value::Float(a), Value::Int(b)) => {
            apply_sort_direction(a.partial_cmp(&(*b as f64)).unwrap_or(Ordering::Equal), asc)
        }
        (Value::Float(a), Value::Float(b)) => {
            apply_sort_direction(a.partial_cmp(b).unwrap_or(Ordering::Equal), asc)
        }
        (Value::String(a), Value::String(b)) => {
            let ordering = match string_comparison_mode {
                StringComparisonMode::CaseInsensitive => compare_text_case_insensitive(a, b),
                StringComparisonMode::CaseSensitive => a.cmp(b),
            };

            apply_sort_direction(ordering, asc)
        }
        (Value::Bool(a), Value::Bool(b)) => apply_sort_direction(a.cmp(b), asc),
        _ => Ordering::Equal,
    }
}

fn apply_sort_direction(ordering: Ordering, asc: bool) -> Ordering {
    if asc {
        ordering
    } else {
        ordering.reverse()
    }
}

fn order_value_kind(value: &Value) -> Option<OrderValueKind> {
    match value {
        Value::Int(_) | Value::Float(_) => Some(OrderValueKind::Number),
        Value::String(_) => Some(OrderValueKind::String),
        Value::Bool(_) => Some(OrderValueKind::Bool),
        Value::Null => None,
    }
}

#[derive(Debug)]
struct SortableRow {
    row: Row,
    keys: Vec<Value>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OrderValueKind {
    Number,
    String,
    Bool,
}
