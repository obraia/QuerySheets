use super::MockSource;
use crate::{QueryEngine, QueryError, SqlLikeQueryEngine};
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
        .execute(&source, "SELECT name FROM spreadsheet WHERE age > 15")
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
            "SELECT name AS person, age + 5 AS adjusted_age, 1 AS constante FROM spreadsheet WHERE name = 'bia'",
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
            "SELECT name AS person, age + 1 AS adjusted_age, age * 2 FROM spreadsheet",
        )
        .expect("query should execute");

    let header = execution
        .schema
        .columns
        .iter()
        .map(|column| column.name.clone())
        .collect::<Vec<_>>();
    let rows = execution.rows.collect::<Vec<_>>();

    assert_eq!(header, vec!["person", "adjusted_age", "age * 2"]);
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
        .execute(&source, "SELECT name FROM spreadsheet WHERE name = 'BIA'")
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
        .execute(&source, "SELECT name FROM spreadsheet WHERE name = 'BIA'")
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values, vec![Value::String("BIA".into())]);
}

#[test]
fn executes_where_with_float_and_null_without_error() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("time")]),
        rows: vec![
            Row::new(vec![Value::Float(12.0)]),
            Row::new(vec![Value::Null]),
            Row::new(vec![Value::Float(5.0)]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine
        .execute(&source, "SELECT time FROM spreadsheet WHERE time >= 10")
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values, vec![Value::Float(12.0)]);
}

#[test]
fn executes_where_null_literal_comparisons_as_non_matching() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("time")]),
        rows: vec![
            Row::new(vec![Value::Float(12.0)]),
            Row::new(vec![Value::Null]),
        ],
    };

    let engine = SqlLikeQueryEngine;

    let eq_result = engine
        .execute(&source, "SELECT time FROM spreadsheet WHERE time = NULL")
        .expect("query should execute")
        .collect::<Vec<_>>();
    assert!(eq_result.is_empty());

    let neq_result = engine
        .execute(&source, "SELECT time FROM spreadsheet WHERE time != NULL")
        .expect("query should execute")
        .collect::<Vec<_>>();
    assert!(neq_result.is_empty());
}

#[test]
fn executes_where_like_prefix_suffix_and_middle_patterns() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("name")]),
        rows: vec![
            Row::new(vec![Value::String("ana".into())]),
            Row::new(vec![Value::String("bia".into())]),
            Row::new(vec![Value::String("caio".into())]),
        ],
    };

    let engine = SqlLikeQueryEngine;

    let prefix_result = engine
        .execute(&source, "SELECT name FROM spreadsheet WHERE name LIKE 'bi%'")
        .expect("query should execute")
        .collect::<Vec<_>>();
    assert_eq!(prefix_result.len(), 1);
    assert_eq!(prefix_result[0].values, vec![Value::String("bia".into())]);

    let suffix_result = engine
        .execute(&source, "SELECT name FROM spreadsheet WHERE name LIKE '%io'")
        .expect("query should execute")
        .collect::<Vec<_>>();
    assert_eq!(suffix_result.len(), 1);
    assert_eq!(suffix_result[0].values, vec![Value::String("caio".into())]);

    let middle_result = engine
        .execute(&source, "SELECT name FROM spreadsheet WHERE name LIKE 'b%a'")
        .expect("query should execute")
        .collect::<Vec<_>>();
    assert_eq!(middle_result.len(), 1);
    assert_eq!(middle_result[0].values, vec![Value::String("bia".into())]);
}

#[test]
fn executes_where_like_single_character_and_not_like() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("name")]),
        rows: vec![
            Row::new(vec![Value::String("ana".into())]),
            Row::new(vec![Value::String("bia".into())]),
            Row::new(vec![Value::String("caio".into())]),
        ],
    };

    let engine = SqlLikeQueryEngine;

    let single_char_result = engine
        .execute(&source, "SELECT name FROM spreadsheet WHERE name LIKE '_ia'")
        .expect("query should execute")
        .collect::<Vec<_>>();
    assert_eq!(single_char_result.len(), 1);
    assert_eq!(single_char_result[0].values, vec![Value::String("bia".into())]);

    let not_like_result = engine
        .execute(
            &source,
            "SELECT name FROM spreadsheet WHERE name NOT LIKE '%ia%' ORDER BY name",
        )
        .expect("query should execute")
        .collect::<Vec<_>>();
    assert_eq!(not_like_result.len(), 2);
    assert_eq!(not_like_result[0].values, vec![Value::String("ana".into())]);
    assert_eq!(not_like_result[1].values, vec![Value::String("caio".into())]);
}

#[test]
fn executes_where_like_case_insensitive_by_default() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("name")]),
        rows: vec![
            Row::new(vec![Value::String("ana".into())]),
            Row::new(vec![Value::String("bia".into())]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine
        .execute(&source, "SELECT name FROM spreadsheet WHERE name LIKE 'B%'")
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values, vec![Value::String("bia".into())]);
}

#[test]
fn executes_where_like_case_sensitive_when_enabled() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("name")]),
        rows: vec![
            Row::new(vec![Value::String("bia".into())]),
            Row::new(vec![Value::String("Bia".into())]),
        ],
    };

    let engine = SqlLikeQueryEngine.with_case_sensitive_strings(true);
    let result = engine
        .execute(&source, "SELECT name FROM spreadsheet WHERE name LIKE 'B%'")
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values, vec![Value::String("Bia".into())]);
}

#[test]
fn executes_where_like_null_as_non_matching() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("name")]),
        rows: vec![
            Row::new(vec![Value::Null]),
            Row::new(vec![Value::String("bia".into())]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine
        .execute(&source, "SELECT name FROM spreadsheet WHERE name LIKE 'b%'")
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values, vec![Value::String("bia".into())]);
}

#[test]
fn returns_error_for_like_variants_outside_scope() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("name")]),
        rows: vec![Row::new(vec![Value::String("bia".into())])],
    };

    let engine = SqlLikeQueryEngine;

    let ilike_result =
        engine.execute(&source, "SELECT name FROM spreadsheet WHERE name ILIKE 'b%'");
    match ilike_result {
        Err(QueryError::UnsupportedWhere(_)) => {}
        Err(other) => panic!("unexpected error: {other}"),
        Ok(_) => panic!("expected unsupported where error"),
    }

    let escape_result = engine.execute(
        &source,
        "SELECT name FROM spreadsheet WHERE name LIKE 'b!%' ESCAPE '!'",
    );
    match escape_result {
        Err(QueryError::UnsupportedWhere(_)) => {}
        Err(other) => panic!("unexpected error: {other}"),
        Ok(_) => panic!("expected unsupported where error"),
    }
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
        .execute(&source, "SELECT name FROM spreadsheet WHERE age IN (10, 30)")
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
        .execute(&source, "SELECT name FROM spreadsheet WHERE age NOT IN (10, 30)")
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
        .execute(&source, "SELECT name FROM spreadsheet WHERE name IN ('BIA', 'CAIO')")
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
        .execute(&source, "SELECT name FROM spreadsheet WHERE age NOT IN (10, NULL)")
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert!(result.is_empty());
}

#[test]
fn executes_where_in_subquery_numeric() {
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
        .execute(
            &source,
            "SELECT name FROM spreadsheet WHERE age IN (SELECT age FROM spreadsheet WHERE age >= 20)",
        )
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values, vec![Value::String("bia".into())]);
    assert_eq!(result[1].values, vec![Value::String("caio".into())]);
}

#[test]
fn executes_where_not_in_subquery_numeric() {
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
        .execute(
            &source,
            "SELECT name FROM spreadsheet WHERE age NOT IN (SELECT age FROM spreadsheet WHERE age >= 20)",
        )
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values, vec![Value::String("ana".into())]);
}

#[test]
fn returns_error_when_in_subquery_returns_multiple_columns() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("name"), Column::new("age")]),
        rows: vec![
            Row::new(vec![Value::String("ana".into()), Value::Int(10)]),
            Row::new(vec![Value::String("bia".into()), Value::Int(20)]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine.execute(
        &source,
        "SELECT name FROM spreadsheet WHERE age IN (SELECT age, name FROM spreadsheet)",
    );

    match result {
        Err(QueryError::UnsupportedWhere(_)) => {}
        Err(other) => panic!("unexpected error: {other}"),
        Ok(_) => panic!("expected unsupported where error"),
    }
}

#[test]
fn executes_scalar_subquery_in_projection() {
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
            "SELECT name, (SELECT age FROM spreadsheet WHERE name = 'bia') AS bia_age FROM spreadsheet WHERE name = 'ana'",
        )
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 1);
    assert_eq!(
        result[0].values,
        vec![Value::String("ana".into()), Value::Int(20)]
    );
}

#[test]
fn returns_error_when_scalar_subquery_returns_multiple_rows() {
    let source = MockSource {
        schema: Schema::new(vec![Column::new("name"), Column::new("age")]),
        rows: vec![
            Row::new(vec![Value::String("ana".into()), Value::Int(10)]),
            Row::new(vec![Value::String("bia".into()), Value::Int(20)]),
        ],
    };

    let engine = SqlLikeQueryEngine;
    let result = engine.execute(
        &source,
        "SELECT name, (SELECT age FROM spreadsheet) AS any_age FROM spreadsheet WHERE name = 'ana'",
    );

    match result {
        Err(QueryError::UnsupportedSelect(_)) => {}
        Err(other) => panic!("unexpected error: {other}"),
        Ok(_) => panic!("expected unsupported select error"),
    }
}

#[test]
fn executes_order_by_with_scalar_subquery_projection_by_position() {
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
        .execute(
            &source,
            "SELECT name, (SELECT p2.age FROM spreadsheet p2 WHERE p2.name = spreadsheet.name) AS own_age FROM spreadsheet ORDER BY 2 DESC",
        )
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].values, vec![Value::String("bia".into()), Value::Int(30)]);
    assert_eq!(result[1].values, vec![Value::String("caio".into()), Value::Int(20)]);
    assert_eq!(result[2].values, vec![Value::String("ana".into()), Value::Int(10)]);
}

#[test]
fn executes_order_by_non_projected_column_with_scalar_subquery_projection() {
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
        .execute(
            &source,
            "SELECT name, (SELECT p2.age FROM spreadsheet p2 WHERE p2.name = spreadsheet.name) AS own_age FROM spreadsheet ORDER BY age DESC",
        )
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].values, vec![Value::String("bia".into()), Value::Int(30)]);
    assert_eq!(result[1].values, vec![Value::String("caio".into()), Value::Int(20)]);
    assert_eq!(result[2].values, vec![Value::String("ana".into()), Value::Int(10)]);
}

#[test]
fn executes_where_exists_non_correlated() {
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
        .execute(
            &source,
            "SELECT name FROM spreadsheet WHERE EXISTS (SELECT 1 FROM spreadsheet WHERE age > 20) ORDER BY name",
        )
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 3);
    assert_eq!(result[0].values, vec![Value::String("ana".into())]);
    assert_eq!(result[1].values, vec![Value::String("bia".into())]);
    assert_eq!(result[2].values, vec![Value::String("caio".into())]);
}

#[test]
fn executes_where_not_exists_non_correlated() {
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
        .execute(
            &source,
            "SELECT name FROM spreadsheet WHERE NOT EXISTS (SELECT 1 FROM spreadsheet WHERE age > 20)",
        )
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert!(result.is_empty());
}

#[test]
fn executes_where_exists_correlated() {
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
        .execute(
            &source,
            "SELECT name FROM spreadsheet p WHERE EXISTS (SELECT 1 FROM spreadsheet p2 WHERE p2.age = p.age AND p2.age > 20)",
        )
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 1);
    assert_eq!(result[0].values, vec![Value::String("caio".into())]);
}

#[test]
fn executes_where_not_exists_correlated() {
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
        .execute(
            &source,
            "SELECT name FROM spreadsheet p WHERE NOT EXISTS (SELECT 1 FROM spreadsheet p2 WHERE p2.age = p.age AND p2.age > 20) ORDER BY name",
        )
        .expect("query should execute")
        .collect::<Vec<_>>();

    assert_eq!(result.len(), 2);
    assert_eq!(result[0].values, vec![Value::String("ana".into())]);
    assert_eq!(result[1].values, vec![Value::String("bia".into())]);
}
