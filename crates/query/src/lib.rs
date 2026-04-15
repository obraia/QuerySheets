use query_sheets_core::{DataSource, Row, Schema};

mod aggregation;
mod errors;
mod expr;
mod parser;
mod projection;

pub use errors::QueryError;
pub use parser::extract_table_name;

use aggregation::{
    build_group_by_aggregation_plan, execute_group_by_aggregation, extract_group_by_column_indexes,
};
use expr::eval_predicate;
use projection::{build_projection, project_row};

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
        let parsed_select = parser::parse_select(query)?;
        let schema = source.schema().clone();

        if let Some(group_by_columns) = extract_group_by_column_indexes(&schema, &parsed_select.group_by)? {
            let plan =
                build_group_by_aggregation_plan(&schema, &parsed_select.projection, &group_by_columns)?;
            return execute_group_by_aggregation(source, &schema, parsed_select.selection.as_ref(), plan);
        }

        let (projection, projected_schema) = build_projection(&schema, &parsed_select.projection)?;
        let where_expr = parsed_select.selection;

        let iter = source.scan().filter_map(move |row| {
            if let Some(expr) = &where_expr {
                let keep = eval_predicate(expr, &row, &schema).unwrap_or(false);
                if !keep {
                    return None;
                }
            }

            let values = project_row(&projection, &row, &schema);

            Some(Row::new(values))
        });

        Ok(QueryExecution {
            schema: projected_schema,
            rows: Box::new(iter),
        })
    }
}

#[cfg(test)]
mod tests;
