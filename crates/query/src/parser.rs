use crate::errors::QueryError;
use sqlparser::ast::{
    Expr, OrderByExpr, Query, Select, SetExpr, Statement, TableFactor, UnaryOperator, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct Pagination {
    pub limit: Option<usize>,
    pub offset: usize,
}

#[derive(Debug, Clone)]
pub(crate) struct ParsedSelect {
    pub select: Select,
    pub order_by: Vec<OrderByExpr>,
    pub pagination: Pagination,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableReference {
    pub schema: Option<String>,
    pub table: String,
}

pub fn extract_table_reference(sql: &str) -> Result<Option<TableReference>, QueryError> {
    let parsed = parse_select(sql)?;
    table_reference_from_select(&parsed.select)
}

pub fn extract_table_name(sql: &str) -> Result<Option<String>, QueryError> {
    Ok(extract_table_reference(sql)?.map(|table_ref| table_ref.table))
}

pub(crate) fn parse_select(sql: &str) -> Result<ParsedSelect, QueryError> {
    let dialect = GenericDialect {};
    let statements = Parser::parse_sql(&dialect, sql).map_err(|err| QueryError::Parse(err.to_string()))?;
    let statement = statements.first().ok_or(QueryError::UnsupportedStatement)?;

    let Statement::Query(query) = statement else {
        return Err(QueryError::UnsupportedStatement);
    };

    select_from_query(query)
}

fn select_from_query(query: &Query) -> Result<ParsedSelect, QueryError> {
    let SetExpr::Select(select) = &*query.body else {
        return Err(QueryError::UnsupportedQuery);
    };

    if select.from.is_empty() {
        return Err(QueryError::MissingFrom);
    }

    let pagination = parse_pagination(query)?;
    let order_by = parse_order_by(query)?;

    Ok(ParsedSelect {
        select: (**select).clone(),
        order_by,
        pagination,
    })
}

fn table_reference_from_select(select: &Select) -> Result<Option<TableReference>, QueryError> {
    let Some(table) = select.from.first() else {
        return Ok(None);
    };

    match &table.relation {
        TableFactor::Table { name, .. } => {
            let Some(table_ident) = name.0.last() else {
                return Ok(None);
            };

            let schema = if name.0.len() >= 2 {
                Some(name.0[name.0.len() - 2].value.clone())
            } else {
                None
            };

            Ok(Some(TableReference {
                schema,
                table: table_ident.value.clone(),
            }))
        }
        _ => Err(QueryError::UnsupportedQuery),
    }
}

fn parse_order_by(query: &Query) -> Result<Vec<OrderByExpr>, QueryError> {
    let Some(order_by) = &query.order_by else {
        return Ok(Vec::new());
    };

    if order_by.interpolate.is_some() {
        return Err(QueryError::UnsupportedOrderBy(
            "INTERPOLATE is not supported".to_string(),
        ));
    }

    for item in &order_by.exprs {
        if item.with_fill.is_some() {
            return Err(QueryError::UnsupportedOrderBy(format!(
                "{} WITH FILL is not supported",
                item.expr
            )));
        }
    }

    Ok(order_by.exprs.clone())
}

fn parse_pagination(query: &Query) -> Result<Pagination, QueryError> {
    let limit = query
        .limit
        .as_ref()
        .map(|expr| parse_positive_integer_clause(expr, "LIMIT"))
        .transpose()?;

    let offset = query
        .offset
        .as_ref()
        .map(|offset| parse_non_negative_integer_clause(&offset.value, "OFFSET"))
        .transpose()?
        .unwrap_or(0);

    Ok(Pagination { limit, offset })
}

fn parse_positive_integer_clause(expr: &Expr, clause: &str) -> Result<usize, QueryError> {
    let value = parse_non_negative_integer_clause(expr, clause)?;

    if value == 0 {
        return Err(QueryError::UnsupportedPagination(format!(
            "{clause} must be greater than zero"
        )));
    }

    Ok(value)
}

fn parse_non_negative_integer_clause(expr: &Expr, clause: &str) -> Result<usize, QueryError> {
    let Some(number) = integer_literal(expr) else {
        return Err(QueryError::UnsupportedPagination(format!(
            "{clause} must be a non-negative integer literal"
        )));
    };

    if number.starts_with('-') {
        return Err(QueryError::UnsupportedPagination(format!(
            "{clause} must be a non-negative integer literal"
        )));
    }

    number.parse::<usize>().map_err(|_| {
        QueryError::UnsupportedPagination(format!(
            "{clause} value '{number}' is not a valid non-negative integer"
        ))
    })
}

fn integer_literal(expr: &Expr) -> Option<&str> {
    match expr {
        Expr::Value(Value::Number(number, _)) => Some(number.as_str()),
        Expr::UnaryOp {
            op: UnaryOperator::Plus,
            expr,
        } => integer_literal(expr),
        _ => None,
    }
}