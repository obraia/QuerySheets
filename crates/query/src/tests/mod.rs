use query_sheets_core::{DataSource, Row, Schema};

mod aggregation_error_tests;
mod aggregation_success_tests;
mod join_tests;
mod order_by_tests;
mod pagination_tests;
mod select_tests;

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
