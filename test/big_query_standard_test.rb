require 'test-unit'
require_relative '../lib/relationizer/big_query/standard.rb'

class BigQueryStandardTest < Test::Unit::TestCase
  include Relationizer::BigQuery::Standard

  TEST_CASES = {
    "BigQuery Standard SQL case 1" => [
      { id: nil, name: nil },
      [[1, 'hoge'], [2, 'fuga']],
      %Q{SELECT * FROM UNNEST(ARRAY<STRUCT<id INT64, name STRING>>[(1, 'hoge'), (2, 'fuga')])}
    ],
    "Array column" => [
      { id: nil, name: nil, combination: nil },
      [[1, 'hoge', [1, 2, 3]], [2, 'fuga', [4, 5, 6]]],
      %Q{SELECT * } +
      %Q{FROM UNNEST(ARRAY<STRUCT<id INT64, name STRING, combination ARRAY<INT64>>>} +
      %Q{[(1, 'hoge', [1, 2, 3]), (2, 'fuga', [4, 5, 6])])}
    ],
    "FLOAT column" => [
      { id: nil, ratio: nil },
      [[1, 1.002], [2, 3.14]],
      %Q{SELECT * FROM UNNEST(ARRAY<STRUCT<id INT64, ratio FLOAT64>>[(1, 1.002), (2, 3.14)])}
    ],
    "DATE column" => [
      { id: nil, birthday: nil },
      [[1, Date.parse('1999-02-11')], [2, Date.parse('2000-01-15')]],
      %Q{SELECT * FROM UNNEST(ARRAY<STRUCT<id INT64, birthday DATE>>}+
      %Q{[(1, '1999-02-11'), (2, '2000-01-15')])}
    ],
    "TIMESTAMP column" => [
      { id: nil, created_at: nil },
      [
        [1, DateTime.parse('1999-02-11 12:00:33')],
        [2, DateTime.parse('2000-01-15 17:45:11')]
      ],
      %Q{SELECT * FROM UNNEST(ARRAY<STRUCT<id INT64, created_at TIMESTAMP>>}+
      %Q{[(1, '1999-02-11 12:00:33'), (2, '2000-01-15 17:45:11')])}
    ],
    "Single column relation" => [
      { id: nil },
      [[1], [2], [3]],
      %Q{SELECT id FROM UNNEST(ARRAY<STRUCT<id INT64, ___dummy STRING>>} +
      %Q{[(1, NULL), (2, NULL), (3, NULL)])}
    ]
  }

  data(TEST_CASES)
  test "Test" do |(schema, tuples, expected)|
    assert_equal(create_relation_literal(schema, tuples), expected)
  end
end
