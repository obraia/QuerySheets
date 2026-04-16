use query_sheets_core::{DataSource, Row, Schema, Value};
use sqlparser::ast::{BinaryOperator, Expr, JoinConstraint, JoinOperator, Select, TableFactor};
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
use expr::{eval_predicate, resolve_column, resolve_compound_column};
use ordering::{apply_order_by_to_execution, execute_select_with_order_by};
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
