# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What is Relationizer

Ruby gem that converts `Array<Array>` into SQL relation literals. Two backends:
- **`Relationizer::BigQuery`** → `SELECT * FROM UNNEST(ARRAY<STRUCT<...>>[...])`
- **`Relationizer::Postgresql`** → `SELECT "col"::TYPE FROM (VALUES(...)) AS t("col")`

Both are modules intended to be `include`d. The public API is `create_relation_literal(schema, tuples)`.

## Commands

```bash
bundle install            # Install dependencies
bundle exec rake          # Run all tests (default task)
bundle exec rake test     # Run all tests (explicit)
bin/console               # IRB with gem loaded
```

## Architecture

```
lib/relationizer.rb            # Namespace module, requires submodules
lib/relationizer/version.rb    # VERSION constant
lib/relationizer/big_query.rb  # BigQuery SQL generation (module)
lib/relationizer/postgresql.rb # PostgreSQL SQL generation (module)
```

Each backend module:
- Infers SQL types from Ruby objects (Integer→INT64/INT8, String→STRING/TEXT, etc.)
- Supports manual type specification via schema hash values
- Has custom error classes (`ReasonlessTypeError`, `TypeNotFoundError`)
- Handles edge cases: NULL, single-column relations (BigQuery adds dummy column), empty tuples, Infinity

## Testing

- Framework: **test-unit** (~> 3.0.0)
- Test files: `test/*_test.rb`
- Helper: `test/to_one_line.rb` — refinement that normalizes multi-line SQL for assertion comparison
- Tests use `data` method for parameterized test cases

## Notes

- Zero runtime dependencies (pure Ruby)
- Column names are quoted (`"col"` for PG, `` `col` `` for BQ)
- String escaping differs per backend (doubled single quotes for PG, backslash escaping for BQ)
