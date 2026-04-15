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
mod tests {
    use super::{QueryEngine, QueryError, SqlLikeQueryEngine};
    use query_sheets_core::{Column, DataSource, Row, Schema, Value};

    struct MockSource {
        schema: Schema,
        rows: Vec<Row>,
    }

    impl DataSource for MockSource {
        fn schema(&self) -> &Schema {
            &self.schema
        }

        fn scan<'a>(&'a self) -> Box<dyn Iterator<Item = Row> + 'a> {
            Box::new(self.rows.iter().cloned())
        }
    }

    #[test]
    fn executes_select_where_projection() {
        let source = MockSource {
            schema: Schema::new(vec![Column::new("name"), Column::new("age")]),
            rows: vec![
                Row::new(vec![Value::String("ana".into()), Value::Int(10)]),
                Row::new(vec![Value::String("bia".into()), Value::Int(20)]),
                Row::new(vec![Value::String("caio".into()), Value::Int(30)]),
            ],
        };

        let engine = SqlLikeQueryEngine;
        let result = engine
            .execute(&source, "SELECT name FROM planilha WHERE age > 15")
            .expect("query should execute")
            .collect::<Vec<_>>();

        assert_eq!(result.len(), 2);
        assert_eq!(result[0].values, vec![Value::String("bia".into())]);
        assert_eq!(result[1].values, vec![Value::String("caio".into())]);
    }

    #[test]
    fn executes_projection_with_alias_and_expression() {
        let source = MockSource {
            schema: Schema::new(vec![Column::new("name"), Column::new("age")]),
            rows: vec![
                Row::new(vec![Value::String("ana".into()), Value::Int(10)]),
                Row::new(vec![Value::String("bia".into()), Value::Int(20)]),
            ],
        };

        let engine = SqlLikeQueryEngine;
        let result = engine
            .execute(
                &source,
                "SELECT name AS pessoa, age + 5 AS idade_ajustada, 1 AS constante FROM planilha WHERE name = 'bia'",
            )
            .expect("query should execute")
            .collect::<Vec<_>>();

        assert_eq!(result.len(), 1);
        assert_eq!(
            result[0].values,
            vec![Value::String("bia".into()), Value::Int(25), Value::Int(1)]
        );
    }

    #[test]
    fn returns_projected_schema_with_aliases() {
        let source = MockSource {
            schema: Schema::new(vec![Column::new("name"), Column::new("age")]),
            rows: vec![Row::new(vec![Value::String("bia".into()), Value::Int(20)])],
        };

        let engine = SqlLikeQueryEngine;
        let execution = engine
            .execute_with_schema(
                &source,
                "SELECT name AS pessoa, age + 1 AS idade_ajustada, age * 2 FROM planilha",
            )
            .expect("query should execute");

        let header = execution
            .schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let rows = execution.rows.collect::<Vec<_>>();

        assert_eq!(header, vec!["pessoa", "idade_ajustada", "age * 2"]);
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].values,
            vec![Value::String("bia".into()), Value::Int(21), Value::Int(40)]
        );
    }

    #[test]
    fn executes_group_by_with_count_and_aliases() {
        let source = MockSource {
            schema: Schema::new(vec![Column::new("name"), Column::new("segment")]),
            rows: vec![
                Row::new(vec![Value::String("ana".into()), Value::String("Enterprise".into())]),
                Row::new(vec![Value::String("bia".into()), Value::String("SMB".into())]),
                Row::new(vec![Value::String("caio".into()), Value::String("Enterprise".into())]),
            ],
        };

        let engine = SqlLikeQueryEngine;
        let execution = engine
            .execute_with_schema(
                &source,
                "SELECT segment AS customer_segment, COUNT(*) AS total FROM planilha GROUP BY segment",
            )
            .expect("query should execute");

        let header = execution
            .schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let rows = execution.rows.collect::<Vec<_>>();

        assert_eq!(header, vec!["customer_segment", "total"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].values,
            vec![Value::String("Enterprise".into()), Value::Int(2)]
        );
        assert_eq!(rows[1].values, vec![Value::String("SMB".into()), Value::Int(1)]);
    }

    #[test]
    fn executes_group_by_with_count_sum_and_avg() {
        let source = MockSource {
            schema: Schema::new(vec![Column::new("segment"), Column::new("revenue")]),
            rows: vec![
                Row::new(vec![Value::String("Enterprise".into()), Value::Int(120)]),
                Row::new(vec![Value::String("Enterprise".into()), Value::Int(91)]),
                Row::new(vec![Value::String("SMB".into()), Value::Int(50)]),
            ],
        };

        let engine = SqlLikeQueryEngine;
        let execution = engine
            .execute_with_schema(
                &source,
                "SELECT segment, COUNT(*) AS total_customers, SUM(revenue) AS total_revenue, AVG(revenue) AS avg_revenue FROM planilha GROUP BY segment",
            )
            .expect("query should execute");

        let header = execution
            .schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let rows = execution.rows.collect::<Vec<_>>();

        assert_eq!(header, vec!["segment", "total_customers", "total_revenue", "avg_revenue"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].values,
            vec![
                Value::String("Enterprise".into()),
                Value::Int(2),
                Value::Int(211),
                Value::Float(105.5),
            ]
        );
        assert_eq!(
            rows[1].values,
            vec![
                Value::String("SMB".into()),
                Value::Int(1),
                Value::Int(50),
                Value::Float(50.0),
            ]
        );
    }

    #[test]
    fn executes_group_by_with_min_and_max() {
        let source = MockSource {
            schema: Schema::new(vec![Column::new("segment"), Column::new("revenue")]),
            rows: vec![
                Row::new(vec![Value::String("Enterprise".into()), Value::Int(120)]),
                Row::new(vec![Value::String("Enterprise".into()), Value::Int(91)]),
                Row::new(vec![Value::String("SMB".into()), Value::Int(50)]),
            ],
        };

        let engine = SqlLikeQueryEngine;
        let execution = engine
            .execute_with_schema(
                &source,
                "SELECT segment, MIN(revenue) AS min_revenue, MAX(revenue) AS max_revenue FROM planilha GROUP BY segment",
            )
            .expect("query should execute");

        let header = execution
            .schema
            .columns
            .iter()
            .map(|column| column.name.clone())
            .collect::<Vec<_>>();
        let rows = execution.rows.collect::<Vec<_>>();

        assert_eq!(header, vec!["segment", "min_revenue", "max_revenue"]);
        assert_eq!(rows.len(), 2);
        assert_eq!(
            rows[0].values,
            vec![Value::String("Enterprise".into()), Value::Int(91), Value::Int(120)]
        );
        assert_eq!(
            rows[1].values,
            vec![Value::String("SMB".into()), Value::Int(50), Value::Int(50)]
        );
    }

    #[test]
    fn returns_error_when_sum_targets_non_numeric_column() {
        let source = MockSource {
            schema: Schema::new(vec![Column::new("segment"), Column::new("name")]),
            rows: vec![
                Row::new(vec![
                    Value::String("Enterprise".into()),
                    Value::String("ana".into()),
                ]),
                Row::new(vec![Value::String("SMB".into()), Value::String("bia".into())]),
            ],
        };

        let engine = SqlLikeQueryEngine;
        let err = match engine.execute(
            &source,
            "SELECT segment, SUM(name) FROM planilha GROUP BY segment",
        ) {
            Ok(_) => panic!("query should fail"),
            Err(err) => err,
        };

        match err {
            QueryError::UnsupportedSelect(message) => {
                assert!(message.contains("SUM(name)"));
                assert!(message.contains("numeric"));
            }
            other => panic!("expected UnsupportedSelect error, got {other:?}"),
        }
    }

    #[test]
    fn returns_error_when_min_targets_mixed_incomparable_values() {
        let source = MockSource {
            schema: Schema::new(vec![Column::new("segment"), Column::new("value")]),
            rows: vec![
                Row::new(vec![Value::String("Enterprise".into()), Value::Int(120)]),
                Row::new(vec![
                    Value::String("Enterprise".into()),
                    Value::String("outlier".into()),
                ]),
            ],
        };

        let engine = SqlLikeQueryEngine;
        let err = match engine.execute(
            &source,
            "SELECT segment, MIN(value) FROM planilha GROUP BY segment",
        ) {
            Ok(_) => panic!("query should fail"),
            Err(err) => err,
        };

        match err {
            QueryError::UnsupportedSelect(message) => {
                assert!(message.contains("MIN(value)"));
                assert!(message.contains("comparable values"));
            }
            other => panic!("expected UnsupportedSelect error, got {other:?}"),
        }
    }

    #[test]
    fn returns_error_when_projection_has_non_grouped_column_in_group_query() {
        let source = MockSource {
            schema: Schema::new(vec![Column::new("name"), Column::new("age")]),
            rows: vec![
                Row::new(vec![Value::String("ana".into()), Value::Int(10)]),
                Row::new(vec![Value::String("bia".into()), Value::Int(20)]),
            ],
        };

        let engine = SqlLikeQueryEngine;
        let err = match engine.execute(
            &source,
            "SELECT name, age, COUNT(*) FROM planilha GROUP BY name",
        ) {
            Ok(_) => panic!("query should fail"),
            Err(err) => err,
        };

        match err {
            QueryError::UnsupportedSelect(message) => assert!(message.contains("age")),
            other => panic!("expected UnsupportedSelect error, got {other:?}"),
        }
    }
}
