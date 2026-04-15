use crate::expr::{eval_predicate, resolve_column, resolve_compound_column};
use crate::projection::projection_output_name;
use crate::{QueryError, QueryExecution};
use query_sheets_core::{Column, DataSource, Row, Schema, Value};
use sqlparser::ast::{
    Expr, Function, FunctionArg, FunctionArgExpr, FunctionArguments, GroupByExpr, SelectItem,
};

#[derive(Debug, Clone)]
pub(crate) enum AggregationSelectItem {
    GroupKey(usize),
    CountRows,
    SumColumn {
        column_index: usize,
        expression: String,
    },
    AvgColumn {
        column_index: usize,
        expression: String,
    },
    MinColumn {
        column_index: usize,
        expression: String,
    },
    MaxColumn {
        column_index: usize,
        expression: String,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct GroupByAggregationPlan {
    key_column_indexes: Vec<usize>,
    select_items: Vec<AggregationSelectItem>,
    output_schema: Schema,
}

pub(crate) fn extract_group_by_column_indexes(
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
                    Expr::CompoundIdentifier(identifiers) => {
                        resolve_compound_column(schema, identifiers)?
                    }
                    _ => return Err(QueryError::UnsupportedSelect(expr.to_string())),
                };
                indexes.push(index);
            }

            Ok(Some(indexes))
        }
    }
}

pub(crate) fn build_group_by_aggregation_plan(
    schema: &Schema,
    select_items: &[SelectItem],
    group_by_column_indexes: &[usize],
) -> Result<GroupByAggregationPlan, QueryError> {
    if select_items.is_empty() {
        return Err(QueryError::UnsupportedSelect("projection is empty".to_string()));
    }

    let mut plan_items = Vec::with_capacity(select_items.len());
    let mut output_columns = Vec::with_capacity(select_items.len());
    let mut has_aggregate = false;

    for item in select_items {
        match item {
            SelectItem::UnnamedExpr(expr) => {
                let plan_item = parse_group_select_expr(schema, expr, group_by_column_indexes)?;
                if matches!(plan_item, AggregationSelectItem::CountRows)
                    || matches!(plan_item, AggregationSelectItem::SumColumn { .. })
                    || matches!(plan_item, AggregationSelectItem::AvgColumn { .. })
                    || matches!(plan_item, AggregationSelectItem::MinColumn { .. })
                    || matches!(plan_item, AggregationSelectItem::MaxColumn { .. })
                {
                    has_aggregate = true;
                }

                plan_items.push(plan_item);
                output_columns.push(Column::new(projection_output_name(expr)));
            }
            SelectItem::ExprWithAlias { expr, alias } => {
                let plan_item = parse_group_select_expr(schema, expr, group_by_column_indexes)?;
                if matches!(plan_item, AggregationSelectItem::CountRows)
                    || matches!(plan_item, AggregationSelectItem::SumColumn { .. })
                    || matches!(plan_item, AggregationSelectItem::AvgColumn { .. })
                    || matches!(plan_item, AggregationSelectItem::MinColumn { .. })
                    || matches!(plan_item, AggregationSelectItem::MaxColumn { .. })
                {
                    has_aggregate = true;
                }

                plan_items.push(plan_item);
                output_columns.push(Column::new(alias.value.clone()));
            }
            other => {
                return Err(QueryError::UnsupportedSelect(other.to_string()));
            }
        }
    }

    if !has_aggregate {
        return Err(QueryError::UnsupportedSelect(
            "GROUP BY queries currently require at least one aggregate in projection".to_string(),
        ));
    }

    Ok(GroupByAggregationPlan {
        key_column_indexes: group_by_column_indexes.to_vec(),
        select_items: plan_items,
        output_schema: Schema::new(output_columns),
    })
}

pub(crate) fn execute_group_by_aggregation<'a>(
    source: &'a dyn DataSource,
    schema: &Schema,
    where_expr: Option<&Expr>,
    plan: GroupByAggregationPlan,
) -> Result<QueryExecution<'a>, QueryError> {
    let GroupByAggregationPlan {
        key_column_indexes,
        select_items,
        output_schema,
    } = plan;

    let mut groups: Vec<GroupByState> = Vec::new();

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

        if let Some(group_state) = groups.iter_mut().find(|state| state.key_values == key_values) {
            apply_row_to_group_state(&select_items, &mut group_state.aggregation_states, &row)?;
        } else {
            let mut aggregation_states = initial_group_state(&select_items);
            apply_row_to_group_state(&select_items, &mut aggregation_states, &row)?;
            groups.push(GroupByState {
                key_values,
                aggregation_states,
            });
        }
    }

    let rows = groups.into_iter().map(move |group_state| {
        let values = select_items
            .iter()
            .zip(group_state.aggregation_states.iter())
            .map(|(item, state)| match (item, state) {
                (AggregationSelectItem::GroupKey(key_index), _) => group_state
                    .key_values
                    .get(*key_index)
                    .cloned()
                    .unwrap_or(Value::Null),
                (AggregationSelectItem::CountRows, GroupAggregationState::CountRows(count)) => {
                    Value::Int(*count)
                }
                (
                    AggregationSelectItem::SumColumn { .. },
                    GroupAggregationState::Sum(sum_accumulator),
                ) => sum_accumulator.to_value(),
                (
                    AggregationSelectItem::AvgColumn { .. },
                    GroupAggregationState::Avg(avg_accumulator),
                ) => avg_accumulator.to_value(),
                (
                    AggregationSelectItem::MinColumn { .. },
                    GroupAggregationState::Min(min_accumulator),
                ) => min_accumulator.to_value(),
                (
                    AggregationSelectItem::MaxColumn { .. },
                    GroupAggregationState::Max(max_accumulator),
                ) => max_accumulator.to_value(),
                _ => Value::Null,
            })
            .collect::<Vec<_>>();

        Row::new(values)
    });

    Ok(QueryExecution {
        schema: output_schema,
        rows: Box::new(rows),
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

            if let Some((aggregate_kind, column_index)) =
                parse_single_column_aggregate_argument(schema, function, expr)?
            {
                return match aggregate_kind {
                    AggregateFunctionKind::Sum => Ok(AggregationSelectItem::SumColumn {
                        column_index,
                        expression: expr.to_string(),
                    }),
                    AggregateFunctionKind::Avg => Ok(AggregationSelectItem::AvgColumn {
                        column_index,
                        expression: expr.to_string(),
                    }),
                    AggregateFunctionKind::Min => Ok(AggregationSelectItem::MinColumn {
                        column_index,
                        expression: expr.to_string(),
                    }),
                    AggregateFunctionKind::Max => Ok(AggregationSelectItem::MaxColumn {
                        column_index,
                        expression: expr.to_string(),
                    }),
                };
            }

            Err(QueryError::UnsupportedSelect(expr.to_string()))
        }
        _ => Err(QueryError::UnsupportedSelect(expr.to_string())),
    }
}

fn parse_single_column_aggregate_argument(
    schema: &Schema,
    function: &Function,
    original_expr: &Expr,
) -> Result<Option<(AggregateFunctionKind, usize)>, QueryError> {
    let Some(function_name) = function.name.0.last() else {
        return Ok(None);
    };

    let aggregate_kind = if function_name.value.eq_ignore_ascii_case("sum") {
        AggregateFunctionKind::Sum
    } else if function_name.value.eq_ignore_ascii_case("avg") {
        AggregateFunctionKind::Avg
    } else if function_name.value.eq_ignore_ascii_case("min") {
        AggregateFunctionKind::Min
    } else if function_name.value.eq_ignore_ascii_case("max") {
        AggregateFunctionKind::Max
    } else {
        return Ok(None);
    };

    if function.filter.is_some() || function.over.is_some() || !function.within_group.is_empty() {
        return Err(QueryError::UnsupportedSelect(original_expr.to_string()));
    }

    if !matches!(function.parameters, FunctionArguments::None) {
        return Err(QueryError::UnsupportedSelect(original_expr.to_string()));
    }

    let FunctionArguments::List(arg_list) = &function.args else {
        return Err(QueryError::UnsupportedSelect(original_expr.to_string()));
    };

    if arg_list.duplicate_treatment.is_some()
        || !arg_list.clauses.is_empty()
        || arg_list.args.len() != 1
    {
        return Err(QueryError::UnsupportedSelect(original_expr.to_string()));
    }

    let FunctionArg::Unnamed(FunctionArgExpr::Expr(argument_expr)) = &arg_list.args[0] else {
        return Err(QueryError::UnsupportedSelect(original_expr.to_string()));
    };

    let column_index = match argument_expr {
        Expr::Identifier(identifier) => resolve_column(schema, identifier)?,
        Expr::CompoundIdentifier(identifiers) => resolve_compound_column(schema, identifiers)?,
        _ => return Err(QueryError::UnsupportedSelect(original_expr.to_string())),
    };

    Ok(Some((aggregate_kind, column_index)))
}

#[derive(Debug, Clone, Copy)]
enum AggregateFunctionKind {
    Sum,
    Avg,
    Min,
    Max,
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

    if arg_list.duplicate_treatment.is_some()
        || !arg_list.clauses.is_empty()
        || arg_list.args.len() != 1
    {
        return false;
    }

    matches!(
        &arg_list.args[0],
        FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
    )
}

#[derive(Debug, Clone)]
struct GroupByState {
    key_values: Vec<Value>,
    aggregation_states: Vec<GroupAggregationState>,
}

#[derive(Debug, Clone)]
enum GroupAggregationState {
    GroupKey,
    CountRows(i64),
    Sum(SumAccumulator),
    Avg(AvgAccumulator),
    Min(MinMaxAccumulator),
    Max(MinMaxAccumulator),
}

#[derive(Debug, Clone, Default)]
struct SumAccumulator {
    int_sum: i64,
    float_sum: f64,
    has_float: bool,
    has_value: bool,
}

impl SumAccumulator {
    fn add_value(&mut self, value: &Value, expression: &str) -> Result<(), QueryError> {
        match value {
            Value::Int(v) => {
                self.has_value = true;
                if self.has_float {
                    self.float_sum += *v as f64;
                } else {
                    self.int_sum = self.int_sum.checked_add(*v).ok_or_else(|| {
                        QueryError::UnsupportedSelect(format!("{expression} overflowed i64 during SUM"))
                    })?;
                }
                Ok(())
            }
            Value::Float(v) => {
                self.has_value = true;
                if !self.has_float {
                    self.float_sum = self.int_sum as f64;
                    self.has_float = true;
                }
                self.float_sum += *v;
                Ok(())
            }
            Value::Null => Ok(()),
            _ => Err(QueryError::UnsupportedSelect(format!(
                "{expression} requires numeric values"
            ))),
        }
    }

    fn to_value(&self) -> Value {
        if !self.has_value {
            return Value::Null;
        }

        if self.has_float {
            return Value::Float(self.float_sum);
        }

        Value::Int(self.int_sum)
    }
}

#[derive(Debug, Clone, Default)]
struct AvgAccumulator {
    sum: f64,
    count: i64,
}

#[derive(Debug, Clone, Default)]
struct MinMaxAccumulator {
    current: Option<Value>,
}

impl MinMaxAccumulator {
    fn add_value(&mut self, value: &Value, expression: &str, find_max: bool) -> Result<(), QueryError> {
        if matches!(value, Value::Null) {
            return Ok(());
        }

        let Some(current) = self.current.as_ref() else {
            self.current = Some(value.clone());
            return Ok(());
        };

        let ordering = compare_aggregate_values(current, value).map_err(|reason| {
            QueryError::UnsupportedSelect(format!(
                "{expression} requires comparable values, got {reason}"
            ))
        })?;

        let should_replace = if find_max {
            ordering == std::cmp::Ordering::Less
        } else {
            ordering == std::cmp::Ordering::Greater
        };

        if should_replace {
            self.current = Some(value.clone());
        }

        Ok(())
    }

    fn to_value(&self) -> Value {
        self.current.clone().unwrap_or(Value::Null)
    }
}

fn compare_aggregate_values(left: &Value, right: &Value) -> Result<std::cmp::Ordering, String> {
    match (left, right) {
        (Value::Int(a), Value::Int(b)) => Ok(a.cmp(b)),
        (Value::Float(a), Value::Float(b)) => a
            .partial_cmp(b)
            .ok_or_else(|| "NaN encountered in floating-point comparison".to_string()),
        (Value::Int(a), Value::Float(b)) => (*a as f64)
            .partial_cmp(b)
            .ok_or_else(|| "NaN encountered in floating-point comparison".to_string()),
        (Value::Float(a), Value::Int(b)) => a
            .partial_cmp(&(*b as f64))
            .ok_or_else(|| "NaN encountered in floating-point comparison".to_string()),
        (Value::String(a), Value::String(b)) => Ok(a.cmp(b)),
        (Value::Bool(a), Value::Bool(b)) => Ok(a.cmp(b)),
        _ => Err(format!("'{left:?}' and '{right:?}' are not comparable")),
    }
}

impl AvgAccumulator {
    fn add_value(&mut self, value: &Value, expression: &str) -> Result<(), QueryError> {
        match value {
            Value::Int(v) => {
                self.sum += *v as f64;
                self.count += 1;
                Ok(())
            }
            Value::Float(v) => {
                self.sum += *v;
                self.count += 1;
                Ok(())
            }
            Value::Null => Ok(()),
            _ => Err(QueryError::UnsupportedSelect(format!(
                "{expression} requires numeric values"
            ))),
        }
    }

    fn to_value(&self) -> Value {
        if self.count == 0 {
            return Value::Null;
        }

        Value::Float(self.sum / self.count as f64)
    }
}

fn initial_group_state(select_items: &[AggregationSelectItem]) -> Vec<GroupAggregationState> {
    select_items
        .iter()
        .map(|item| match item {
            AggregationSelectItem::GroupKey(_) => GroupAggregationState::GroupKey,
            AggregationSelectItem::CountRows => GroupAggregationState::CountRows(0),
            AggregationSelectItem::SumColumn { .. } => {
                GroupAggregationState::Sum(SumAccumulator::default())
            }
            AggregationSelectItem::AvgColumn { .. } => {
                GroupAggregationState::Avg(AvgAccumulator::default())
            }
            AggregationSelectItem::MinColumn { .. } => {
                GroupAggregationState::Min(MinMaxAccumulator::default())
            }
            AggregationSelectItem::MaxColumn { .. } => {
                GroupAggregationState::Max(MinMaxAccumulator::default())
            }
        })
        .collect::<Vec<_>>()
}

fn apply_row_to_group_state(
    select_items: &[AggregationSelectItem],
    aggregation_states: &mut [GroupAggregationState],
    row: &Row,
) -> Result<(), QueryError> {
    for (item, state) in select_items.iter().zip(aggregation_states.iter_mut()) {
        match (item, state) {
            (AggregationSelectItem::GroupKey(_), _) => {}
            (AggregationSelectItem::CountRows, GroupAggregationState::CountRows(count)) => {
                *count += 1;
            }
            (
                AggregationSelectItem::SumColumn {
                    column_index,
                    expression,
                },
                GroupAggregationState::Sum(sum_accumulator),
            ) => {
                let value = row.values.get(*column_index).unwrap_or(&Value::Null);
                sum_accumulator.add_value(value, expression)?;
            }
            (
                AggregationSelectItem::AvgColumn {
                    column_index,
                    expression,
                },
                GroupAggregationState::Avg(avg_accumulator),
            ) => {
                let value = row.values.get(*column_index).unwrap_or(&Value::Null);
                avg_accumulator.add_value(value, expression)?;
            }
            (
                AggregationSelectItem::MinColumn {
                    column_index,
                    expression,
                },
                GroupAggregationState::Min(min_accumulator),
            ) => {
                let value = row.values.get(*column_index).unwrap_or(&Value::Null);
                min_accumulator.add_value(value, expression, false)?;
            }
            (
                AggregationSelectItem::MaxColumn {
                    column_index,
                    expression,
                },
                GroupAggregationState::Max(max_accumulator),
            ) => {
                let value = row.values.get(*column_index).unwrap_or(&Value::Null);
                max_accumulator.add_value(value, expression, true)?;
            }
            _ => {}
        }
    }

    Ok(())
}