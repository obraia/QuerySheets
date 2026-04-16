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
fn executes_case_insensitive_string_join() {
    let customers = MockSource {
        schema: Schema::new(vec![Column::new("customer_id"), Column::new("name")]),
        rows: vec![
            Row::new(vec![Value::String("c-1".into()), Value::String("Ana".into())]),
            Row::new(vec![Value::String("c-2".into()), Value::String("Bia".into())]),
        ],
    };

    let orders = ResolvedTableData {
        schema: Schema::new(vec![Column::new("customer_id"), Column::new("amount")]),
        rows: vec![
            Row::new(vec![Value::String("C-1".into()), Value::Int(100)]),
            Row::new(vec![Value::String("C-2".into()), Value::Int(50)]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine
        .execute_with_schema_and_resolver(
            &customers,
            "SELECT c.name, o.amount FROM Customers c JOIN Orders o ON c.customer_id = o.customer_id ORDER BY o.amount",
            |_| Ok(orders.clone()),
        )
        .expect("join query should execute")
        .rows
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values, vec![Value::String("Bia".into()), Value::Int(50)]);
    assert_eq!(result[1].values, vec![Value::String("Ana".into()), Value::Int(100)]);
}

#[test]
fn executes_join_with_mixed_int_and_float_keys() {
    let vehicles = MockSource {
        schema: Schema::new(vec![Column::new("model_id"), Column::new("plate")]),
        rows: vec![
            Row::new(vec![Value::Int(1), Value::String("AAA1A11".into())]),
            Row::new(vec![Value::Int(2), Value::String("BBB2B22".into())]),
        ],
    };

    let models = ResolvedTableData {
        schema: Schema::new(vec![Column::new("model_id"), Column::new("description")]),
        rows: vec![
            Row::new(vec![Value::Float(1.0), Value::String("Sedan".into())]),
            Row::new(vec![Value::Float(2.0), Value::String("SUV".into())]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine
        .execute_with_schema_and_resolver(
            &vehicles,
            "SELECT v.plate, m.description FROM Vehicles v JOIN Models m ON v.model_id = m.model_id ORDER BY v.plate",
            |_| Ok(models.clone()),
        )
        .expect("join query should execute")
        .rows
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 2);
    assert_eq!(
        result[0].values,
        vec![Value::String("AAA1A11".into()), Value::String("Sedan".into())]
    );
    assert_eq!(
        result[1].values,
        vec![Value::String("BBB2B22".into()), Value::String("SUV".into())]
    );
}

#[test]
fn executes_join_with_null_join_keys_without_error() {
    let vehicles = MockSource {
        schema: Schema::new(vec![Column::new("model_id"), Column::new("plate")]),
        rows: vec![
            Row::new(vec![Value::Int(1), Value::String("AAA1A11".into())]),
            Row::new(vec![Value::Null, Value::String("ZZZ9Z99".into())]),
        ],
    };

    let models = ResolvedTableData {
        schema: Schema::new(vec![Column::new("model_id"), Column::new("description")]),
        rows: vec![
            Row::new(vec![Value::Int(1), Value::String("Sedan".into())]),
            Row::new(vec![Value::Null, Value::String("Unknown".into())]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine
        .execute_with_schema_and_resolver(
            &vehicles,
            "SELECT v.plate, m.description FROM Vehicles v LEFT JOIN Models m ON v.model_id = m.model_id ORDER BY v.plate",
            |_| Ok(models.clone()),
        )
        .expect("join query should execute")
        .rows
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 2);
    assert_eq!(
        result[0].values,
        vec![Value::String("AAA1A11".into()), Value::String("Sedan".into())]
    );
    assert_eq!(
        result[1].values,
        vec![Value::String("ZZZ9Z99".into()), Value::Null]
    );
}

#[test]
fn executes_inner_join_with_smaller_left_side() {
    let vehicles = MockSource {
        schema: Schema::new(vec![Column::new("model_id"), Column::new("plate")]),
        rows: vec![
            Row::new(vec![Value::Int(1), Value::String("AAA1A11".into())]),
            Row::new(vec![Value::Int(2), Value::String("BBB2B22".into())]),
        ],
    };

    let models = ResolvedTableData {
        schema: Schema::new(vec![Column::new("model_id"), Column::new("description")]),
        rows: vec![
            Row::new(vec![Value::Int(1), Value::String("Sedan".into())]),
            Row::new(vec![Value::Int(1), Value::String("Sedan Plus".into())]),
            Row::new(vec![Value::Int(2), Value::String("SUV".into())]),
            Row::new(vec![Value::Int(3), Value::String("Truck".into())]),
            Row::new(vec![Value::Int(4), Value::String("Coupe".into())]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine
        .execute_with_schema_and_resolver(
            &vehicles,
            "SELECT v.plate, m.description FROM Vehicles v JOIN Models m ON v.model_id = m.model_id ORDER BY v.plate, m.description",
            |_| Ok(models.clone()),
        )
        .expect("join query should execute")
        .rows
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 3);
    assert_eq!(
        result[0].values,
        vec![Value::String("AAA1A11".into()), Value::String("Sedan".into())]
    );
    assert_eq!(
        result[1].values,
        vec![Value::String("AAA1A11".into()), Value::String("Sedan Plus".into())]
    );
    assert_eq!(
        result[2].values,
        vec![Value::String("BBB2B22".into()), Value::String("SUV".into())]
    );
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
fn returns_error_for_unknown_alias_in_projection_with_join() {
    let vehicles = MockSource {
        schema: Schema::new(vec![
            Column::new("codigo_modelo"),
            Column::new("codigo_cor"),
            Column::new("placa"),
        ]),
        rows: vec![Row::new(vec![
            Value::Int(1),
            Value::Int(10),
            Value::String("ZZZ5498".into()),
        ])],
    };

    let models = ResolvedTableData {
        schema: Schema::new(vec![Column::new("codigo_modelo"), Column::new("descricao")]),
        rows: vec![Row::new(vec![Value::Int(1), Value::String("Sedan".into())])],
    };

    let colors = ResolvedTableData {
        schema: Schema::new(vec![Column::new("codigo_cor"), Column::new("descricao")]),
        rows: vec![Row::new(vec![Value::Int(10), Value::String("Preto".into())])],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine.execute_with_schema_and_resolver(
        &vehicles,
        "SELECT v.placa, m.descricao AS modelo, a.descricao AS cor FROM veiculos v JOIN modelos m ON v.codigo_modelo = m.codigo_modelo JOIN cores c ON c.codigo_cor = v.codigo_cor WHERE v.placa IN ('ZZZ5498') LIMIT 10",
        |table_ref| {
            if table_ref.table.eq_ignore_ascii_case("modelos") {
                Ok(models.clone())
            } else if table_ref.table.eq_ignore_ascii_case("cores") {
                Ok(colors.clone())
            } else {
                Err(QueryError::TableResolution(table_ref.table.clone()))
            }
        },
    );

    match result {
        Err(QueryError::UnknownTableAlias(alias)) if alias.eq_ignore_ascii_case("a") => {}
        Err(other) => panic!("unexpected error: {other}"),
        Ok(_) => panic!("expected unknown table alias error"),
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

#[test]
fn returns_error_for_reflexive_join_predicate() {
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
        "SELECT c.name FROM Customers c JOIN Orders o ON c.customer_id = c.customer_id",
        |_| Ok(orders.clone()),
    );

    match result {
        Err(QueryError::InvalidJoinCondition(_)) => {}
        Err(other) => panic!("unexpected error: {other}"),
        Ok(_) => panic!("expected invalid join condition error"),
    }
}
