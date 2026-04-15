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
- Projected schema support (used by CLI header output)
- CLI with `query` command, `--sheet`, and `--header`
- Integration tests with generated `.xlsx` fixtures

Not implemented yet:
- Aggregations (`GROUP BY`, `COUNT`, `SUM`, etc.)
- CSV/JSON export
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

Force worksheet selection (overrides `FROM` sheet name):

```bash
cargo run -p query-sheets-cli -- query \
  --file ./planilha.xlsx \
  --sheet Archive \
  --sql "SELECT CustomerId, CustomerName FROM Customers WHERE AccountStatus = 'Inactive'" \
  --header
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

Not supported yet:
- joins
- subqueries
- `GROUP BY`
- aggregate functions
- ordering and pagination

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
- Aggregations (`GROUP BY`)
- CSV/JSON export

Phase 3:
- Node bindings
- Parallelism

Phase 4:
- Custom Excel parser
- Advanced optimizations (mmap, XML parsing, string cache, columnar ideas)

## Known Constraints

- The adapter currently uses `calamine`, with dependency fully isolated to adapter crate.
- Current implementation is iterator-driven and avoids extra data copies in query pipeline, but full file-level streaming behavior still depends on adapter internals and future parser strategy.

## Documentation Changelog

Use this section to keep documentation changes visible over time.

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
