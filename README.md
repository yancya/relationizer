# Relationizer

A Ruby gem that converts `Array<Array>` into SQL relation literals. Supports BigQuery and PostgreSQL.

## Installation

```ruby
gem 'relationizer'
```

```
$ bundle install
```

## Usage

`include` the backend module you need and call `create_relation_literal(schema, tuples)`.

- `schema` — A Hash of column names to types. Set the value to `nil` to auto-infer the type from the tuples.
- `tuples` — Row data as `Array<Array>`.

### BigQuery

```ruby
require 'relationizer/big_query'

class MyQuery
  include Relationizer::BigQuery
end

q = MyQuery.new

q.create_relation_literal(
  { id: nil, name: nil },
  [[1, 'hoge'], [2, 'fuga']]
)
#=> "SELECT * FROM UNNEST(ARRAY<STRUCT<`id` INT64, `name` STRING>>[(1, 'hoge'), (2, 'fuga')])"
```

#### Auto type inference

| Ruby type             | BigQuery type |
|-----------------------|---------------|
| `Integer`             | INT64         |
| `Float` / `BigDecimal` | FLOAT64     |
| `String`              | STRING        |
| `TrueClass` / `FalseClass` | BOOL    |
| `Time` / `DateTime`   | TIMESTAMP    |
| `Date`                | DATE          |
| `Array`               | ARRAY<T>     |

#### Manual type specification

Pass a Symbol as the schema value to override auto-inference. Useful when a column contains mixed types or when tuples are empty.

```ruby
# Force ratio column to FLOAT64 (mixed Integer and Float)
q.create_relation_literal(
  { id: nil, ratio: :FLOAT64 },
  [[1, 1], [2, 3.14]]
)
#=> "SELECT * FROM UNNEST(ARRAY<STRUCT<`id` INT64, `ratio` FLOAT64>>[(1, 1), (2, 3.14)])"

# Empty tuples (manual type specification is required)
q.create_relation_literal(
  { id: :INT64, name: :STRING },
  []
)
#=> "SELECT * FROM UNNEST(ARRAY<STRUCT<`id` INT64, `name` STRING>>[])"
```

#### Array columns

```ruby
q.create_relation_literal(
  { id: nil, name: nil, combination: nil },
  [[1, 'hoge', [1, 2, 3]], [2, 'fuga', [4, 5, 6]]]
)
#=> "SELECT * FROM UNNEST(ARRAY<STRUCT<`id` INT64, `name` STRING, `combination` ARRAY<INT64>>>[(1, 'hoge', [1, 2, 3]), (2, 'fuga', [4, 5, 6])])"
```

#### Single column

BigQuery does not support single-column STRUCTs in UNNEST, so a dummy column is added internally. Only the original column is returned in the SELECT.

```ruby
q.create_relation_literal(
  { id: nil },
  [[1], [2], [3]]
)
#=> "SELECT id FROM UNNEST(ARRAY<STRUCT<`id` INT64, `___dummy` STRING>>[(1, NULL), (2, NULL), (3, NULL)])"
```

### PostgreSQL

```ruby
require 'relationizer/postgresql'

class MyQuery
  include Relationizer::Postgresql
end

q = MyQuery.new

q.create_relation_literal(
  { id: nil, name: nil },
  [[1, 'hoge'], [2, 'fuga']]
)
#=> %Q{SELECT "id"::INT8, "name"::TEXT FROM (VALUES('1', 'hoge'), ('2', 'fuga')) AS t("id", "name")}
```

#### Auto type inference

| Ruby type             | PostgreSQL type |
|-----------------------|-----------------|
| `Integer`             | INT8            |
| `Float`               | FLOAT8          |
| `BigDecimal`          | DECIMAL         |
| `String`              | TEXT            |
| `TrueClass` / `FalseClass` | BOOLEAN   |
| `Time` / `DateTime`   | TIMESTAMPTZ    |
| `Date`                | DATE            |

#### NULL

`nil` values are converted to SQL `NULL`.

```ruby
q.create_relation_literal(
  { id: nil },
  [[1], [nil]]
)
#=> %Q{SELECT "id"::INT8 FROM (VALUES('1'), (NULL)) AS t("id")}
```

## Errors

- `ReasonlessTypeError` — Raised when types are mixed within a single column (e.g. Integer and String in the same column)
- `TypeNotFoundError` (BigQuery only) — Raised when tuples are empty and types are not manually specified

## License

[MIT License](http://opensource.org/licenses/MIT)
