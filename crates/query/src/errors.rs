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