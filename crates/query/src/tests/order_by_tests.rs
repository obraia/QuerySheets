use super::MockSource;
use crate::{QueryEngine, QueryError, SqlLikeQueryEngine};
use query_sheets_core::{Column, Row, Schema, Value};

#[test]
fn executes_select_with_order_by_desc() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("name"), Column::new("age")]),
        rows: vec![
            Row::new(vec![Value::String("ana".into()), Value::Int(10)]),
            Row::new(vec![Value::String("bia".into()), Value::Int(30)]),
            Row::new(vec![Value::String("caio".into()), Value::Int(20)]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine
        .execute(&source, "SELECT name, age FROM planilha ORDER BY age DESC")
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].values, vec![Value::String("bia".into()), Value::Int(30)]);
    assert_eq!(result[1].values, vec![Value::String("caio".into()), Value::Int(20)]);
    assert_eq!(result[2].values, vec![Value::String("ana".into()), Value::Int(10)]);
}

#[test]
fn executes_order_by_string_case_insensitive() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("name")]),
        rows: vec![
            Row::new(vec![Value::String("bruno".into())]),
            Row::new(vec![Value::String("Alice".into())]),
            Row::new(vec![Value::String("carla".into())]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine
        .execute(&source, "SELECT name FROM planilha ORDER BY name ASC")
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].values, vec![Value::String("Alice".into())]);
    assert_eq!(result[1].values, vec![Value::String("bruno".into())]);
    assert_eq!(result[2].values, vec![Value::String("carla".into())]);
}

#[test]
fn executes_order_by_string_case_sensitive_when_enabled() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("name")]),
        rows: vec![
            Row::new(vec![Value::String("apple".into())]),
            Row::new(vec![Value::String("Zebra".into())]),
            Row::new(vec![Value::String("maria".into())]),
        ],
    };

    let engine = SqlLikeQueryEngine.with_case_sensitive_strings(true);
    let result = engine
        .execute(&source, "SELECT name FROM planilha ORDER BY name ASC")
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].values, vec![Value::String("Zebra".into())]);
    assert_eq!(result[1].values, vec![Value::String("apple".into())]);
    assert_eq!(result[2].values, vec![Value::String("maria".into())]);
}

#[test]
fn executes_select_with_order_by_non_projected_column() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("name"), Column::new("age")]),
        rows: vec![
            Row::new(vec![Value::String("ana".into()), Value::Int(10)]),
            Row::new(vec![Value::String("bia".into()), Value::Int(30)]),
            Row::new(vec![Value::String("caio".into()), Value::Int(20)]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine
        .execute(&source, "SELECT name FROM planilha ORDER BY age DESC")
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].values, vec![Value::String("bia".into())]);
    assert_eq!(result[1].values, vec![Value::String("caio".into())]);
    assert_eq!(result[2].values, vec![Value::String("ana".into())]);
}

#[test]
fn executes_group_by_with_order_by_alias_and_limit() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("segment"), Column::new("revenue")]),
        rows: vec![
            Row::new(vec![Value::String("Enterprise".into()), Value::Int(120)]),
            Row::new(vec![Value::String("Enterprise".into()), Value::Int(90)]),
            Row::new(vec![Value::String("SMB".into()), Value::Int(50)]),
            Row::new(vec![Value::String("SMB".into()), Value::Int(40)]),
            Row::new(vec![Value::String("Mid".into()), Value::Int(20)]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let execution = engine
        .execute_with_schema(
            &source,
            "SELECT segment, SUM(revenue) AS total_revenue FROM planilha GROUP BY segment ORDER BY total_revenue DESC LIMIT 2",
        )
        .expect("query should execute");

    let rows = execution.rows.collect::<Vec<_>>();

    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].values,
        vec![Value::String("Enterprise".into()), Value::Int(210)]
    );
    assert_eq!(rows[1].values, vec![Value::String("SMB".into()), Value::Int(90)]);
}

#[test]
fn executes_group_by_with_positional_order_by() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("segment"), Column::new("revenue")]),
        rows: vec![
            Row::new(vec![Value::String("Enterprise".into()), Value::Int(120)]),
            Row::new(vec![Value::String("Enterprise".into()), Value::Int(90)]),
            Row::new(vec![Value::String("SMB".into()), Value::Int(50)]),
            Row::new(vec![Value::String("SMB".into()), Value::Int(40)]),
            Row::new(vec![Value::String("Mid".into()), Value::Int(20)]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let execution = engine
        .execute_with_schema(
            &source,
            "SELECT segment, SUM(revenue) AS total_revenue FROM planilha GROUP BY segment ORDER BY 2 DESC",
        )
        .expect("query should execute");

    let rows = execution.rows.collect::<Vec<_>>();

    assert_eq!(rows.len(), 3);
    assert_eq!(
        rows[0].values,
        vec![Value::String("Enterprise".into()), Value::Int(210)]
    );
    assert_eq!(rows[1].values, vec![Value::String("SMB".into()), Value::Int(90)]);
    assert_eq!(rows[2].values, vec![Value::String("Mid".into()), Value::Int(20)]);
}

#[test]
fn returns_error_when_order_by_has_incomparable_mixed_types() {
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
    let err = match engine.execute(&source, "SELECT value FROM planilha ORDER BY value") {
        Ok(_) => panic!("query should fail"),
        Err(err) => err,
    };

    match err {
        QueryError::UnsupportedOrderBy(message) => {
            assert!(message.contains("value"));
            assert!(message.contains("comparable values"));
        }
        other => panic!("expected UnsupportedOrderBy error, got {other:?}"),
    }
}

#[test]
fn executes_order_by_desc_with_nulls_last() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("tempo")]),
        rows: vec![
            Row::new(vec![Value::String("-".into())]),
            Row::new(vec![Value::String("20".into())]),
            Row::new(vec![Value::String("5".into())]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine
        .execute(
            &source,
            "SELECT tempo FROM planilha ORDER BY CAST(tempo AS INT) DESC NULLS LAST",
        )
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].values, vec![Value::String("20".into())]);
    assert_eq!(result[1].values, vec![Value::String("5".into())]);
    assert_eq!(result[2].values, vec![Value::String("-".into())]);
}

#[test]
fn executes_select_with_positional_order_by() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("name"), Column::new("age")]),
        rows: vec![
            Row::new(vec![Value::String("bia".into()), Value::Int(30)]),
            Row::new(vec![Value::String("ana".into()), Value::Int(30)]),
            Row::new(vec![Value::String("caio".into()), Value::Int(20)]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine
        .execute(&source, "SELECT name, age FROM planilha ORDER BY 2 DESC, 1 ASC")
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].values, vec![Value::String("ana".into()), Value::Int(30)]);
    assert_eq!(result[1].values, vec![Value::String("bia".into()), Value::Int(30)]);
    assert_eq!(result[2].values, vec![Value::String("caio".into()), Value::Int(20)]);
}

#[test]
fn returns_error_when_order_by_position_is_out_of_range() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("name"), Column::new("age")]),
        rows: vec![Row::new(vec![Value::String("ana".into()), Value::Int(10)])],
    };

    let engine = SqlLikeQueryEngine;
    let err = match engine.execute(&source, "SELECT name FROM planilha ORDER BY 2") {
        Ok(_) => panic!("query should fail"),
        Err(err) => err,
    };

    match err {
        QueryError::UnsupportedOrderBy(message) => {
            assert!(message.contains("position 2"));
            assert!(message.contains("out of range"));
        }
        other => panic!("expected UnsupportedOrderBy error, got {other:?}"),
    }
}
