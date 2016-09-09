require 'test-unit'
require_relative '../lib/relationizer/big_query/standard.rb'

class BigQueryStandardTest < Test::Unit::TestCase
  include Relationizer::BigQuery::Standard

  test "BigQuery Standard SQL case 1" do
    schema = { id: nil, name: nil }
    tuples = [[1, 'hoge'], [2, 'fuga']]
    assert_equal(
      create_relation_literal(schema, tuples),
      %Q{SELECT * FROM UNNEST(ARRAY<STRUCT<id INT64, name STRING>>[(1, 'hoge'), (2, 'fuga')])}
    )
  end

  test "Array column" do
    schema = { id: nil, name: nil, combination: nil }
    tuples = [[1, 'hoge', [1, 2, 3]], [2, 'fuga', [4, 5, 6]]]
    assert_equal(
      create_relation_literal(schema, tuples),
      %Q{SELECT * } +
      %Q{FROM UNNEST(ARRAY<STRUCT<id INT64, name STRING, combination ARRAY<INT64>>>} +
      %Q{[(1, 'hoge', [1, 2, 3]), (2, 'fuga', [4, 5, 6])])}
    )
  end

  test "FLOAT column" do
    schema = { id: nil, ratio: nil }
    tuples = [[1, 1.002], [2, 3.14]]
    assert_equal(
      create_relation_literal(schema, tuples),
      %Q{SELECT * FROM UNNEST(ARRAY<STRUCT<id INT64, ratio FLOAT64>>[(1, 1.002), (2, 3.14)])}
    )
  end

  test "DATE column" do
    schema = { id: nil, birthday: nil }
    tuples = [[1, Date.parse('1999-02-11')], [2, Date.parse('2000-01-15')]]
    assert_equal(
      create_relation_literal(schema, tuples),
      %Q{SELECT * FROM UNNEST(ARRAY<STRUCT<id INT64, birthday DATE>>}+
      %Q{[(1, '1999-02-11'), (2, '2000-01-15')])}
    )
  end

  test "TIMESTAMP column" do
    schema = { id: nil, created_at: nil }
    tuples = [
      [1, DateTime.parse('1999-02-11 12:00:33')],
      [2, DateTime.parse('2000-01-15 17:45:11')]
    ]
    assert_equal(
      create_relation_literal(schema, tuples),
      %Q{SELECT * FROM UNNEST(ARRAY<STRUCT<id INT64, created_at TIMESTAMP>>}+
      %Q{[(1, '1999-02-11 12:00:33'), (2, '2000-01-15 17:45:11')])}
    )
  end

  test "Single column relation" do
    schema = { id: nil }
    tuples = [[1], [2], [3]]
    assert_equal(
      create_relation_literal(schema, tuples),
      %Q{SELECT id FROM UNNEST(ARRAY<STRUCT<id INT64, ___dummy STRING>>} +
      %Q{[(1, NULL), (2, NULL), (3, NULL)])}
    )
  end
end
