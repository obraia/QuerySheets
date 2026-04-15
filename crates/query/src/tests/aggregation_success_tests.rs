use super::MockSource;
use crate::{QueryEngine, SqlLikeQueryEngine};
use query_sheets_core::{Column, Row, Schema, Value};

#[test]
fn executes_group_by_with_count_and_aliases() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("name"), Column::new("segment")]),
        rows: vec![
            Row::new(vec![
                Value::String("ana".into()),
                Value::String("Enterprise".into()),
            ]),
            Row::new(vec![Value::String("bia".into()), Value::String("SMB".into())]),
            Row::new(vec![
                Value::String("caio".into()),
                Value::String("Enterprise".into()),
            ]),
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

    assert_eq!(
        header,
        vec!["segment", "total_customers", "total_revenue", "avg_revenue"]
    );
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
        vec![
            Value::String("Enterprise".into()),
            Value::Int(91),
            Value::Int(120)
        ]
    );
    assert_eq!(
        rows[1].values,
        vec![Value::String("SMB".into()), Value::Int(50), Value::Int(50)]
    );
}

#[test]
fn executes_group_by_with_casted_numeric_aggregations() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("segment"), Column::new("tempo")]),
        rows: vec![
            Row::new(vec![Value::String("Enterprise".into()), Value::Int(10)]),
            Row::new(vec![
                Value::String("Enterprise".into()),
                Value::String("-".into()),
            ]),
            Row::new(vec![
                Value::String("Enterprise".into()),
                Value::String("20".into()),
            ]),
            Row::new(vec![Value::String("SMB".into()), Value::String("5.5".into())]),
            Row::new(vec![Value::String("SMB".into()), Value::String("n/a".into())]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let execution = engine
        .execute_with_schema(
            &source,
            "SELECT segment, COUNT(*) AS total_rows, AVG(CAST(tempo AS FLOAT)) AS avg_tempo, SUM(CAST(tempo AS FLOAT)) AS total_tempo, MIN(CAST(tempo AS FLOAT)) AS min_tempo, MAX(CAST(tempo AS FLOAT)) AS max_tempo FROM planilha GROUP BY segment",
        )
        .expect("query should execute");

    let rows = execution.rows.collect::<Vec<_>>();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].values,
        vec![
            Value::String("Enterprise".into()),
            Value::Int(3),
            Value::Float(15.0),
            Value::Float(30.0),
            Value::Float(10.0),
            Value::Float(20.0),
        ]
    );
    assert_eq!(
        rows[1].values,
        vec![
            Value::String("SMB".into()),
            Value::Int(2),
            Value::Float(5.5),
            Value::Float(5.5),
            Value::Float(5.5),
            Value::Float(5.5),
        ]
    );
}

#[test]
fn executes_group_by_with_count_column_ignoring_nulls() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("segment"), Column::new("tempo")]),
        rows: vec![
            Row::new(vec![Value::String("Enterprise".into()), Value::Int(10)]),
            Row::new(vec![Value::String("Enterprise".into()), Value::Null]),
            Row::new(vec![Value::String("SMB".into()), Value::String("5.5".into())]),
            Row::new(vec![Value::String("SMB".into()), Value::Null]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let execution = engine
        .execute_with_schema(
            &source,
            "SELECT segment, COUNT(tempo) AS filled_tempo FROM planilha GROUP BY segment",
        )
        .expect("query should execute");

    let rows = execution.rows.collect::<Vec<_>>();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].values,
        vec![Value::String("Enterprise".into()), Value::Int(1)]
    );
    assert_eq!(
        rows[1].values,
        vec![Value::String("SMB".into()), Value::Int(1)]
    );
}

#[test]
fn executes_group_by_with_count_cast_expression() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("segment"), Column::new("tempo")]),
        rows: vec![
            Row::new(vec![Value::String("Enterprise".into()), Value::Int(10)]),
            Row::new(vec![
                Value::String("Enterprise".into()),
                Value::String("-".into()),
            ]),
            Row::new(vec![
                Value::String("Enterprise".into()),
                Value::String("20".into()),
            ]),
            Row::new(vec![Value::String("SMB".into()), Value::String("5.5".into())]),
            Row::new(vec![Value::String("SMB".into()), Value::String("n/a".into())]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let execution = engine
        .execute_with_schema(
            &source,
            "SELECT segment, COUNT(CAST(tempo AS FLOAT)) AS numeric_tempo_rows FROM planilha GROUP BY segment",
        )
        .expect("query should execute");

    let rows = execution.rows.collect::<Vec<_>>();
    assert_eq!(rows.len(), 2);
    assert_eq!(
        rows[0].values,
        vec![Value::String("Enterprise".into()), Value::Int(2)]
    );
    assert_eq!(
        rows[1].values,
        vec![Value::String("SMB".into()), Value::Int(1)]
    );
}
