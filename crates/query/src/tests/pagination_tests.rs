use super::MockSource;
use crate::{QueryEngine, QueryError, SqlLikeQueryEngine};
use query_sheets_core::{Column, Row, Schema, Value};

#[test]
fn executes_select_with_limit_and_offset() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("name"), Column::new("age")]),
        rows: vec![
            Row::new(vec![Value::String("ana".into()), Value::Int(10)]),
            Row::new(vec![Value::String("bia".into()), Value::Int(20)]),
            Row::new(vec![Value::String("caio".into()), Value::Int(30)]),
            Row::new(vec![Value::String("duda".into()), Value::Int(40)]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine
        .execute(
            &source,
            "SELECT name FROM planilha WHERE age >= 20 LIMIT 2 OFFSET 1",
        )
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values, vec![Value::String("caio".into())]);
    assert_eq!(result[1].values, vec![Value::String("duda".into())]);
}

#[test]
fn executes_group_by_with_limit_and_offset() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("segment"), Column::new("revenue")]),
        rows: vec![
            Row::new(vec![Value::String("Enterprise".into()), Value::Int(120)]),
            Row::new(vec![Value::String("SMB".into()), Value::Int(90)]),
            Row::new(vec![Value::String("Mid".into()), Value::Int(50)]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let execution = engine
        .execute_with_schema(
            &source,
            "SELECT segment, COUNT(*) AS total FROM planilha GROUP BY segment LIMIT 1 OFFSET 1",
        )
        .expect("query should execute");

    let rows = execution.rows.collect::<Vec<_>>();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].values, vec![Value::String("SMB".into()), Value::Int(1)]);
}

#[test]
fn returns_error_when_limit_is_zero() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("name")]),
        rows: vec![Row::new(vec![Value::String("ana".into())])],
    };

    let engine = SqlLikeQueryEngine;
    let err = match engine.execute(&source, "SELECT name FROM planilha LIMIT 0") {
        Ok(_) => panic!("query should fail"),
        Err(err) => err,
    };

    match err {
        QueryError::UnsupportedPagination(message) => {
            assert!(message.contains("LIMIT"));
            assert!(message.contains("greater than zero"));
        }
        other => panic!("expected UnsupportedPagination error, got {other:?}"),
    }
}

#[test]
fn returns_error_when_offset_is_not_integer_literal() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("name")]),
        rows: vec![Row::new(vec![Value::String("ana".into())])],
    };

    let engine = SqlLikeQueryEngine;
    let err = match engine.execute(&source, "SELECT name FROM planilha OFFSET 'one'") {
        Ok(_) => panic!("query should fail"),
        Err(err) => err,
    };

    match err {
        QueryError::UnsupportedPagination(message) => {
            assert!(message.contains("OFFSET"));
            assert!(message.contains("non-negative integer literal"));
        }
        other => panic!("expected UnsupportedPagination error, got {other:?}"),
    }
}
