require 'test-unit'
require 'bigdecimal'
require_relative '../lib/relationizer/big_query'
require_relative '../lib/relationizer/postgresql'
require_relative '../lib/relationizer/mysql'

# Cross-backend edge-case matrix. Each backend module is written and
# maintained independently; these tests pin down which behaviors are
# guaranteed to match across all three, and which are documented,
# intentional divergences (see README "Errors" / backend sections).
class ConsistencyTest < Test::Unit::TestCase
  BACKENDS = {
    "BigQuery" => {
      instance: Object.new.extend(Relationizer::BigQuery),
      mod: Relationizer::BigQuery,
      int_type: :INT64,
      str_type: :STRING,
    },
    "PostgreSQL" => {
      instance: Object.new.extend(Relationizer::Postgresql),
      mod: Relationizer::Postgresql,
      int_type: :INT8,
      str_type: :TEXT,
    },
    "MySQL" => {
      instance: Object.new.extend(Relationizer::MySQL),
      mod: Relationizer::MySQL,
      int_type: :BIGINT,
      str_type: :TEXT,
    },
  }

  data(BACKENDS)
  test "nil value in a tuple renders as SQL NULL (or, for MySQL, the JSON null it's read back from)" do |b|
    sql = b[:instance].create_relation_literal({ id: b[:int_type] }, [[1], [nil]])
    assert_match(/null/i, sql)
  end

  data(BACKENDS)
  test "column with only nils and no explicit type raises ReasonlessTypeError" do |b|
    assert_raise(b[:mod]::ReasonlessTypeError) do
      b[:instance].create_relation_literal({ id: nil }, [[nil], [nil]])
    end
  end

  data(BACKENDS)
  test "empty tuples with explicit types produce a relation, not an exception" do |b|
    sql = b[:instance].create_relation_literal({ id: b[:int_type], name: b[:str_type] }, [])
    assert_kind_of(String, sql)
    refute_empty(sql)
  end

  data(BACKENDS)
  test "empty tuples without explicit types raise TypeNotFoundError" do |b|
    assert_raise(b[:mod]::TypeNotFoundError) do
      b[:instance].create_relation_literal({ id: nil, name: nil }, [])
    end
  end

  data(BACKENDS)
  test "mixed types in a column raise ReasonlessTypeError" do |b|
    assert_raise(b[:mod]::ReasonlessTypeError) do
      b[:instance].create_relation_literal({ id: nil }, [[1], ['2']])
    end
  end

  data(BACKENDS)
  test "single column produces a relation with the column visible in SELECT" do |b|
    sql = b[:instance].create_relation_literal({ id: b[:int_type] }, [[1], [2], [3]])
    assert_match(/id/, sql)
  end

  QUOTE_IN_NAME = {
    "BigQuery"   => { instance: BACKENDS["BigQuery"][:instance],   name: 'i`d', escaped: '`i\`d`' },
    "PostgreSQL" => { instance: BACKENDS["PostgreSQL"][:instance], name: 'i"d', escaped: '"i""d"' },
    "MySQL"      => { instance: BACKENDS["MySQL"][:instance],      name: 'i`d', escaped: '`i``d`' },
  }

  data(QUOTE_IN_NAME)
  test "column name containing the backend's own identifier delimiter is escaped, not broken" do |b|
    sql = b[:instance].create_relation_literal({ b[:name] => nil }, [[1]])
    assert_include(sql, b[:escaped])
  end

  QUOTE_IN_VALUE = {
    "BigQuery"   => { instance: BACKENDS["BigQuery"][:instance],   expected: "'it\\'s'" },
    "PostgreSQL" => { instance: BACKENDS["PostgreSQL"][:instance], expected: "'it''s'" },
    "MySQL"      => { instance: BACKENDS["MySQL"][:instance],      expected: "it''s" },
  }

  data(QUOTE_IN_VALUE)
  test "string value containing a single quote is escaped, not broken" do |b|
    sql = b[:instance].create_relation_literal({ name: nil }, [["it's"]])
    assert_include(sql, b[:expected])
  end

  # Float::INFINITY / NaN: documented, intentional divergence per backend.
  # - PostgreSQL FLOAT8 has literal support for 'Infinity'/'NaN'.
  # - BigQuery FLOAT64 has explicit CAST('inf' AS FLOAT64) handling.
  # - MySQL DOUBLE has no literal for either; the backend raises instead
  #   (see #22).
  test "PostgreSQL renders Infinity as a FLOAT8 literal" do
    pg = BACKENDS["PostgreSQL"][:instance]
    sql = pg.create_relation_literal({ r: nil }, [[Float::INFINITY]])
    assert_include(sql, "'Infinity'")
  end

  test "BigQuery renders Infinity via CAST" do
    bq = BACKENDS["BigQuery"][:instance]
    sql = bq.create_relation_literal({ r: :FLOAT64 }, [[Float::INFINITY]])
    assert_include(sql, "CAST('inf' AS FLOAT64)")
  end

  test "MySQL raises for Infinity (DOUBLE has no representation)" do
    my = BACKENDS["MySQL"][:instance]
    assert_raise(Relationizer::MySQL::ReasonlessTypeError) do
      my.create_relation_literal({ r: nil }, [[Float::INFINITY]])
    end
  end

  # BigDecimal precision: PostgreSQL and MySQL preserve full precision
  # (DECIMAL/NUMERIC are exact types). BigQuery's *default* inferred type
  # for BigDecimal is FLOAT64 (a lossy binary float, by design -- use the
  # NUMERIC manual type override for exact precision on BigQuery).
  HIGH_PRECISION = BigDecimal("123.45678901234567890123456789")

  test "PostgreSQL preserves full BigDecimal precision" do
    pg = BACKENDS["PostgreSQL"][:instance]
    sql = pg.create_relation_literal({ amount: :DECIMAL }, [[HIGH_PRECISION]])
    assert_include(sql, "'0.12345678901234567890123456789e3'")
  end

  test "MySQL preserves full BigDecimal precision" do
    my = BACKENDS["MySQL"][:instance]
    sql = my.create_relation_literal({ amount: nil }, [[HIGH_PRECISION]])
    assert_include(sql, '123.45678901234567890123456789')
  end

  test "BigQuery NUMERIC override preserves the full BigDecimal digit sequence" do
    bq = BACKENDS["BigQuery"][:instance]
    sql = bq.create_relation_literal({ amount: :NUMERIC }, [[HIGH_PRECISION]])
    # BigDecimal#to_s renders in scientific notation (e.g. 0.123...e3);
    # not independently verified against a live BigQuery instance here
    # (no network/billing access in this test suite), but the digit
    # sequence itself is unrounded, unlike the lossy FLOAT64 default.
    assert_include(sql, '0.12345678901234567890123456789e3')
  end
end
