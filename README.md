# QuerySheets

[![CI](https://img.shields.io/github/actions/workflow/status/obraia/QeurySheets/ci.yml?branch=main&label=CI)](https://github.com/obraia/QeurySheets/actions)
[![Release](https://img.shields.io/github/v/release/obraia/QeurySheets?label=release)](https://github.com/obraia/QeurySheets/releases)
[![License](https://img.shields.io/github/license/obraia/QeurySheets)](https://github.com/obraia/QeurySheets/blob/main/LICENSE)

High-performance Rust CLI for querying Excel `.xlsx` files with SQL-like syntax.

QuerySheets is designed to evolve from a practical CLI into a robust data processing engine, with low memory usage, clear architectural boundaries, and pluggable adapters.

## Project Status

Current status: alpha (active development)

Implemented today:
- Multi-crate workspace architecture
- Excel adapter using `calamine` isolated behind adapter boundaries
- SQL-like query execution for `SELECT` + `WHERE`
- Projection aliases in `SELECT ... AS ...`
- Simple arithmetic expressions in projection (`+`, `-`, `*`, `/`, `%`)
- Conversion expressions via `CAST(... AS ...)`
- Projected schema support (used by CLI header output)
- Aggregations with `GROUP BY` using `COUNT(*)`, `COUNT(column)`, `SUM(column)`, `AVG(column)`, `MIN(column)`, and `MAX(column)`
- Ordering with `ORDER BY` (`ASC`/`DESC`, optional `NULLS FIRST`/`NULLS LAST`)
- Pagination with `LIMIT` and `OFFSET`
- CLI with `query` command, `--sheet`, `--header`, and `--case-sensitive-strings`
- CSV/JSON/JSONL export inferred from `--output` extension (`.csv`, `.json`, `.jsonl`)
- Integration tests with generated `.xlsx` fixtures

Not implemented yet:
- Additional aggregations (`STDDEV`, etc.)
- Node bindings (`napi-rs`)
- Parallel execution
- Custom Excel parser

## Why QuerySheets

- Streaming-first mindset
- Core and adapter decoupling (ports and adapters)
- Replaceable external libraries
- Lazy iterator pipeline
- Engine-focused architecture that can outgrow a simple wrapper

## Architecture

Core flow:

`DataSource -> Filter -> Projection -> Output`

Boundaries:
- `core`: data model + interfaces (`DataSource`, `Row`, `Value`, `Schema`)
- `adapters`: external I/O implementations (currently Excel via `calamine`)
- `query`: SQL parser and query execution engine
- `cli`: command-line interface

## Repository Layout

```text
/crates
  /core
  /adapters
  /query
  /cli
```

## Requirements

- Rust stable toolchain (recommended: latest stable)
- Cargo

Check installed versions:

```bash
rustc --version
cargo --version
```

## Build

From repository root:

```bash
cargo check
cargo build
```

## Run

CLI help:

```bash
cargo run -p query-sheets-cli -- --help
```

Query command help:

```bash
cargo run -p query-sheets-cli -- query --help
```

Basic query example:

```bash
cargo run -p query-sheets-cli -- query \
  --file ./planilha.xlsx \
  --sql "SELECT CustomerId, CustomerName FROM Customers WHERE Segment = 'Enterprise'"
```

Use projected headers:

```bash
cargo run -p query-sheets-cli -- query \
  --file ./planilha.xlsx \
  --sql "SELECT CustomerId AS ClientId, CustomerName FROM Customers" \
  --header
```

Export result to CSV:

```bash
cargo run -p query-sheets-cli -- query \
  --file ./planilha.xlsx \
  --sql "SELECT CustomerId, CustomerName, AccountStatus FROM Customers" \
  --output ./customers_export.csv
```

Export result to JSON:

```bash
cargo run -p query-sheets-cli -- query \
  --file ./planilha.xlsx \
  --sql "SELECT CustomerId AS ClientId, 1 + 2 AS PriorityScore FROM Customers" \
  --output ./customers_export.json
```

Export result to JSONL:

```bash
cargo run -p query-sheets-cli -- query \
  --file ./planilha.xlsx \
  --sql "SELECT CustomerId, Segment FROM Customers WHERE AccountStatus = 'Active'" \
  --output ./customers_export.jsonl
```

Force worksheet selection (overrides `FROM` sheet name):

```bash
cargo run -p query-sheets-cli -- query \
  --file ./planilha.xlsx \
  --sheet Archive \
  --sql "SELECT CustomerId, CustomerName FROM Customers WHERE AccountStatus = 'Inactive'" \
  --header
```

Paginate query result with `LIMIT` and `OFFSET`:

```bash
cargo run -p query-sheets-cli -- query \
  --file ./planilha.xlsx \
  --sql "SELECT CustomerId, CustomerName FROM Customers LIMIT 10 OFFSET 20" \
  --header
```

Sort and paginate:

```bash
cargo run -p query-sheets-cli -- query \
  --file ./planilha.xlsx \
  --sql "SELECT CustomerId, CustomerName FROM Customers ORDER BY CustomerName DESC LIMIT 10 OFFSET 0" \
  --header
```

Enable case-sensitive string comparison for `WHERE` and `ORDER BY`:

```bash
cargo run -p query-sheets-cli -- query \
  --file ./planilha.xlsx \
  --sql "SELECT CustomerId, Segment FROM Customers WHERE Segment = 'enterprise'" \
  --header \
  --case-sensitive-strings
```

## SQL Support (Current)

Supported:
- `SELECT` statements
- `WHERE` with:
  - comparisons: `=`, `!=`, `>`, `<`, `>=`, `<=`
  - logical operators: `AND`, `OR`
- Projection:
  - column selection
  - wildcard (`*`)
  - aliases (`AS`)
  - simple arithmetic expressions in projection
  - conversion with `CAST(expression AS type)`
- String comparison semantics:
  - default: string comparisons in `WHERE` are case-insensitive
  - default: string sorting in `ORDER BY` is case-insensitive
  - optional strict mode via CLI flag `--case-sensitive-strings`
- Aggregation:
  - `GROUP BY` with grouped columns in projection
  - `COUNT(*)`
  - `COUNT(column)`
  - `SUM(column)`
  - `AVG(column)`
  - `MIN(column)`
  - `MAX(column)`
  - aggregate arguments can use casted expressions, e.g. `AVG(CAST(Tempo AS FLOAT))`
- Pagination:
  - `LIMIT <positive integer>`
  - `OFFSET <non-negative integer>`
- Ordering:
  - `ORDER BY` with one or more expressions
  - `ASC` / `DESC`
  - optional `NULLS FIRST` / `NULLS LAST`
  - positional ordering by projected columns (e.g. `ORDER BY 1, 2 DESC`)
  - in non-aggregated queries, `ORDER BY` can reference columns not present in `SELECT`

Not supported yet:
- joins
- subqueries
- additional aggregate functions (`COUNT(column)`, `STDDEV`, etc.)

## Testing

Run all tests:

```bash
cargo test
```

Run CLI integration suite only:

```bash
cargo test -p query-sheets-cli
```

Integration tests are organized by intent:
- success paths
- error paths
- worksheet selection behavior

## Roadmap

Phase 1 (in progress):
- Excel read adapter
- CLI basics
- `SELECT` + `WHERE`

Phase 2:
- Aggregations (`GROUP BY`) - current support: `COUNT(*)`, `COUNT(column)`, `SUM(column)`, `AVG(column)`, `MIN(column)`, `MAX(column)`
- export improvements (format options)

Phase 3:
- Node bindings
- Parallelism

Phase 4:
- Custom Excel parser
- Advanced optimizations (mmap, XML parsing, string cache, columnar ideas)

## Known Constraints

- The adapter currently uses `calamine`, with dependency fully isolated to adapter crate.
- Current implementation is iterator-driven and avoids extra data copies in query pipeline, but full file-level streaming behavior still depends on adapter internals and future parser strategy.
- CAST conversion is lenient for parsing failures (e.g., `CAST('-' AS FLOAT)` becomes `NULL`), which helps mixed Excel columns but differs from strict SQL engines.

## Documentation Changelog

Use this section to keep documentation changes visible over time.

- 2026-04-15
  - Added `COUNT(column)` aggregation support (counts non-NULL values).
  - Added query engine and CLI integration tests for `COUNT(column)` scenarios.

- 2026-04-15
  - Added CLI flag `--case-sensitive-strings` to opt into case-sensitive string comparison in `WHERE` and `ORDER BY`.
  - Added query engine and CLI integration tests covering the configurable comparison mode.

- 2026-04-15
  - Changed string comparison behavior to case-insensitive in `WHERE` and `ORDER BY`.
  - Added ASCII fast-path comparator with Unicode fallback to preserve performance.

- 2026-04-15
  - Extended `ORDER BY` support with positional indexes (e.g. `ORDER BY 1, 2 DESC`).
  - In non-aggregated queries, `ORDER BY` now supports columns not projected in `SELECT`.
  - Added query engine and CLI integration tests covering both new ordering behaviors.

- 2026-04-15
  - Added SQL ordering support with `ORDER BY` (`ASC`/`DESC`, optional `NULLS FIRST`/`NULLS LAST`).
  - Added query engine unit tests and CLI integration tests for ORDER BY success and error scenarios.
  - Applied SQL execution order: ORDER BY before LIMIT/OFFSET.

- 2026-04-15
  - Added SQL pagination support with `LIMIT` and `OFFSET`.
  - Added query engine unit tests and CLI integration tests for pagination success and invalid pagination clauses.

- 2026-04-15
  - Implemented Phase 2 start: CSV/JSON/JSONL export in CLI via `--output` extension inference.
  - Added integration tests for export success paths and output extension validation.
  - Implemented streaming JSON array serialization to reduce memory peak on large outputs.
  - Implemented first aggregation slice: `GROUP BY` + `COUNT(*)` in query engine and CLI integration tests.

- 2026-04-15
  - Extended `GROUP BY` aggregation support with `SUM(column)` and `AVG(column)`.
  - Added query engine unit tests and CLI integration tests for `COUNT(*) + SUM + AVG`.
  - Added error coverage for aggregate usage on non-numeric columns.

- 2026-04-15
  - Extended `GROUP BY` aggregation support with `MIN(column)` and `MAX(column)`.
  - Added query engine unit tests and CLI integration tests for `MIN/MAX` scenarios.
  - Added error coverage for incomparable mixed-type values during aggregate comparison.

- 2026-04-15
  - Added `CAST(... AS ...)` support for expression evaluation and projection validation.
  - Enabled aggregate expressions with casted arguments (`SUM/AVG/MIN/MAX` over `CAST(...)`).
  - Added coverage for mixed-type columns using casted aggregates.

- 2026-04-15
  - Added baseline public-facing README structure.
  - Added badges for CI, releases, and license.
  - Added maintenance policy and checklist for periodic updates.

## Keeping This README Updated

For a public CLI project, stale docs quickly become a problem. Use this policy:

Update this README whenever one of the following changes:
- CLI command shape or flags
- SQL capabilities or limitations
- crate structure
- test strategy
- roadmap priorities

Recommended cadence:
- update in the same PR as behavior changes
- perform a monthly docs review

Practical checklist for each update:
1. Verify command examples still run.
2. Verify supported SQL list matches real behavior.
3. Verify roadmap reflects current priorities.
4. Verify status section is honest (implemented vs planned).
5. Add an entry in `Documentation Changelog`.
6. Update the timestamp below.

Badge maintenance note:
- if repository name, owner, or workflow filename changes, update badge URLs at the top of this README.

Last README review: 2026-04-15

## Contributing

Contributions are welcome. For now, prefer issues/PRs focused on:
- query capabilities
- adapter performance and memory behavior
- architecture boundaries and extensibility
- CLI usability and error quality

## License

MIT (workspace-configured)
