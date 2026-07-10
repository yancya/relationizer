# Changelog

## 0.4.0 (2026-02-14)

- Add MySQL 8.0 backend (`Relationizer::MySQL`)
- Add support for NUMERIC data types in the BigQuery backend
- Require Ruby 3.2+; update CI to test Ruby 3.2/3.3/3.4/4.0
- Rewrite README with actual usage examples and API documentation
- Add RubyGems release workflow via Trusted Publisher

## 0.3.1

- Upgrade Rake

## 0.3.0

- Fix type-not-found SQL with empty tuples

## 0.2.3

- Fix BOOL type relation for `BigQuery::Standard` module

## 0.2.2 and earlier

- Initial BigQuery and PostgreSQL backends
