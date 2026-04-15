use crate::expr::{
    eval_value, is_supported_cast_data_type, resolve_column, resolve_compound_column,
    sql_literal_to_value,
};
use crate::QueryError;
use query_sheets_core::{Column, Row, Schema, Value};
use sqlparser::ast::{BinaryOperator, Expr, SelectItem, UnaryOperator};

#[derive(Debug, Clone)]
pub(crate) enum ProjectionItem {
    Wildcard,
    Expr(Expr),
}

pub(crate) fn build_projection(
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

pub(crate) fn project_row(projection: &[ProjectionItem], row: &Row, schema: &Schema) -> Vec<Value> {
    let mut out = Vec::new();

    for item in projection {
        match item {
            ProjectionItem::Wildcard => out.extend(row.values.iter().cloned()),
            ProjectionItem::Expr(expr) => out.push(eval_value(expr, row, schema).unwrap_or(Value::Null)),
        }
    }

    out
}

pub(crate) fn projection_output_name(expr: &Expr) -> String {
    match expr {
        Expr::Identifier(identifier) => identifier.value.clone(),
        Expr::CompoundIdentifier(identifiers) => identifiers
            .last()
            .map(|ident| ident.value.clone())
            .unwrap_or_else(|| expr.to_string()),
        _ => expr.to_string(),
    }
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
        Expr::Cast {
            expr: inner,
            data_type,
            ..
        } => {
            if !is_supported_cast_data_type(data_type) {
                return Err(QueryError::UnsupportedSelect(expr.to_string()));
            }

            validate_projection_expr(schema, inner)
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