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

#[test]
fn executes_where_string_comparison_case_insensitive() {
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
        .execute(&source, "SELECT name FROM planilha WHERE name = 'BIA'")
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values, vec![Value::String("bia".into())]);
}

#[test]
fn executes_where_string_comparison_case_sensitive_when_enabled() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("name"), Column::new("age")]),
        rows: vec![
            Row::new(vec![Value::String("ana".into()), Value::Int(10)]),
            Row::new(vec![Value::String("bia".into()), Value::Int(20)]),
            Row::new(vec![Value::String("BIA".into()), Value::Int(30)]),
        ],
    };

    let engine = SqlLikeQueryEngine.with_case_sensitive_strings(true);
    let result = engine
        .execute(&source, "SELECT name FROM planilha WHERE name = 'BIA'")
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values, vec![Value::String("BIA".into())]);
}

#[test]
fn executes_where_with_float_and_null_without_error() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("tempo")]),
        rows: vec![
            Row::new(vec![Value::Float(12.0)]),
            Row::new(vec![Value::Null]),
            Row::new(vec![Value::Float(5.0)]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine
        .execute(&source, "SELECT tempo FROM planilha WHERE tempo >= 10")
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values, vec![Value::Float(12.0)]);
}

#[test]
fn executes_where_null_literal_comparisons_as_non_matching() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("tempo")]),
        rows: vec![
            Row::new(vec![Value::Float(12.0)]),
            Row::new(vec![Value::Null]),
        ],
    };

    let engine = SqlLikeQueryEngine;

    let eq_result = engine
        .execute(&source, "SELECT tempo FROM planilha WHERE tempo = NULL")
        .expect("query should execute")
        .collect::<Vec<_>>();
    assert!(eq_result.is_empty());

    let neq_result = engine
        .execute(&source, "SELECT tempo FROM planilha WHERE tempo != NULL")
        .expect("query should execute")
        .collect::<Vec<_>>();
    assert!(neq_result.is_empty());
}

#[test]
fn executes_where_in_list_numeric() {
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
        .execute(&source, "SELECT name FROM planilha WHERE age IN (10, 30)")
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values, vec![Value::String("ana".into())]);
    assert_eq!(result[1].values, vec![Value::String("caio".into())]);
}

#[test]
fn executes_where_not_in_list_numeric() {
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
        .execute(&source, "SELECT name FROM planilha WHERE age NOT IN (10, 30)")
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values, vec![Value::String("bia".into())]);
}

#[test]
fn executes_where_in_list_string_case_insensitive() {
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
        .execute(&source, "SELECT name FROM planilha WHERE name IN ('BIA', 'CAIO')")
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values, vec![Value::String("bia".into())]);
    assert_eq!(result[1].values, vec![Value::String("caio".into())]);
}

#[test]
fn executes_where_not_in_with_null_list_as_non_matching() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("name"), Column::new("age")]),
        rows: vec![
            Row::new(vec![Value::String("ana".into()), Value::Int(10)]),
            Row::new(vec![Value::String("bia".into()), Value::Int(20)]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine
        .execute(&source, "SELECT name FROM planilha WHERE age NOT IN (10, NULL)")
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert!(result.is_empty());
}
