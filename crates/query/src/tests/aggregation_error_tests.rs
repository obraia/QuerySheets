use super::MockSource;
use crate::{QueryEngine, QueryError, SqlLikeQueryEngine};
use query_sheets_core::{Column, Row, Schema, Value};

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
