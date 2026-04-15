use super::MockSource;
use crate::{QueryEngine, SqlLikeQueryEngine};
use query_sheets_core::{Column, Row, Schema, Value};

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
