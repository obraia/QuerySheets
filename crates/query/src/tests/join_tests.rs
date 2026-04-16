use super::MockSource;
use crate::{QueryEngine, QueryError, ResolvedTableData, SqlLikeQueryEngine};
use query_sheets_core::{Column, Row, Schema, Value};

#[test]
fn executes_inner_join_with_aliases() {
    let customers = MockSource {
        schema: Schema::new(vec![Column::new("customer_id"), Column::new("name")]),
        rows: vec![
            Row::new(vec![Value::String("C-1".into()), Value::String("Ana".into())]),
            Row::new(vec![Value::String("C-2".into()), Value::String("Bia".into())]),
        ],
    };

    let orders = ResolvedTableData {
        schema: Schema::new(vec![Column::new("customer_id"), Column::new("amount")]),
        rows: vec![
            Row::new(vec![Value::String("C-1".into()), Value::Int(100)]),
            Row::new(vec![Value::String("C-2".into()), Value::Int(50)]),
            Row::new(vec![Value::String("C-1".into()), Value::Int(200)]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine
        .execute_with_schema_and_resolver(
            &customers,
            "SELECT c.name, o.amount FROM Customers c JOIN Orders o ON c.customer_id = o.customer_id ORDER BY o.amount",
            |table_ref| {
                if table_ref.table.eq_ignore_ascii_case("orders") {
                    Ok(orders.clone())
                } else {
                    Err(QueryError::TableResolution(table_ref.table.clone()))
                }
            },
        )
        .expect("join query should execute")
        .rows
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].values, vec![Value::String("Bia".into()), Value::Int(50)]);
    assert_eq!(result[1].values, vec![Value::String("Ana".into()), Value::Int(100)]);
    assert_eq!(result[2].values, vec![Value::String("Ana".into()), Value::Int(200)]);
}

#[test]
fn returns_error_for_ambiguous_unqualified_column_in_join() {
    let customers = MockSource {
        schema: Schema::new(vec![Column::new("customer_id"), Column::new("name")]),
        rows: vec![Row::new(vec![
            Value::String("C-1".into()),
            Value::String("Ana".into()),
        ])],
    };

    let orders = ResolvedTableData {
        schema: Schema::new(vec![Column::new("customer_id"), Column::new("amount")]),
        rows: vec![Row::new(vec![Value::String("C-1".into()), Value::Int(100)])],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine.execute_with_schema_and_resolver(
            &customers,
            "SELECT customer_id FROM Customers c JOIN Orders o ON c.customer_id = o.customer_id",
            |_| Ok(orders.clone()),
        );

    match result {
        Err(QueryError::AmbiguousColumn(_)) => {}
        Err(other) => panic!("unexpected error: {other}"),
        Ok(_) => panic!("expected ambiguous column error"),
    }
}

#[test]
fn executes_left_join_with_unmatched_left_rows() {
    let customers = MockSource {
        schema: Schema::new(vec![Column::new("customer_id"), Column::new("name")]),
        rows: vec![
            Row::new(vec![Value::String("C-1".into()), Value::String("Ana".into())]),
            Row::new(vec![Value::String("C-2".into()), Value::String("Bia".into())]),
            Row::new(vec![Value::String("C-3".into()), Value::String("Carla".into())]),
        ],
    };

    let orders = ResolvedTableData {
        schema: Schema::new(vec![Column::new("customer_id"), Column::new("amount")]),
        rows: vec![
            Row::new(vec![Value::String("C-1".into()), Value::Int(100)]),
            Row::new(vec![Value::String("C-2".into()), Value::Int(90)]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine
        .execute_with_schema_and_resolver(
            &customers,
            "SELECT c.name, o.amount FROM Customers c LEFT JOIN Orders o ON c.customer_id = o.customer_id WHERE c.customer_id = 'C-3'",
            |_| Ok(orders.clone()),
        )
        .expect("left join query should execute")
        .rows
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].values,
        vec![Value::String("Carla".into()), Value::Null]
    );
}

#[test]
fn executes_right_join_with_unmatched_right_rows() {
    let customers = MockSource {
        schema: Schema::new(vec![Column::new("customer_id"), Column::new("name")]),
        rows: vec![
            Row::new(vec![Value::String("C-1".into()), Value::String("Ana".into())]),
            Row::new(vec![Value::String("C-2".into()), Value::String("Bia".into())]),
        ],
    };

    let orders = ResolvedTableData {
        schema: Schema::new(vec![
            Column::new("order_id"),
            Column::new("customer_id"),
            Column::new("amount"),
        ]),
        rows: vec![
            Row::new(vec![
                Value::String("O-100".into()),
                Value::String("C-1".into()),
                Value::Int(100),
            ]),
            Row::new(vec![
                Value::String("O-999".into()),
                Value::String("C-9".into()),
                Value::Int(55),
            ]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine
        .execute_with_schema_and_resolver(
            &customers,
            "SELECT c.name, o.order_id FROM Customers c RIGHT JOIN Orders o ON c.customer_id = o.customer_id WHERE o.customer_id = 'C-9'",
            |_| Ok(orders.clone()),
        )
        .expect("right join query should execute")
        .rows
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values, vec![Value::Null, Value::String("O-999".into())]);
}

#[test]
fn returns_error_for_unsupported_join_kind() {
    let customers = MockSource {
        schema: Schema::new(vec![Column::new("customer_id"), Column::new("name")]),
        rows: vec![Row::new(vec![
            Value::String("C-1".into()),
            Value::String("Ana".into()),
        ])],
    };

    let orders = ResolvedTableData {
        schema: Schema::new(vec![Column::new("customer_id"), Column::new("amount")]),
        rows: vec![Row::new(vec![Value::String("C-1".into()), Value::Int(100)])],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine.execute_with_schema_and_resolver(
            &customers,
            "SELECT c.name FROM Customers c FULL JOIN Orders o ON c.customer_id = o.customer_id",
            |_| Ok(orders.clone()),
        );

    match result {
        Err(QueryError::UnsupportedQuery) => {}
        Err(other) => panic!("unexpected error: {other}"),
        Ok(_) => panic!("expected unsupported join error"),
    }
}
