use crate::errors::QueryError;
use sqlparser::ast::{Query, Select, SetExpr, Statement, TableFactor};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;

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

pub(crate) fn parse_select(sql: &str) -> Result<Select, QueryError> {
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