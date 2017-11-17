require 'test-unit'
require_relative '../lib/relationizer/big_query/standard.rb'
require_relative './to_one_line.rb'

class BigQueryStandardTest < Test::Unit::TestCase
  include Relationizer::BigQuery::Standard
  using ToOneLine

  NORMAL_TEST_CASES = {
    "BigQuery Standard SQL case 1" => [
      {
        id: nil,
        name: nil
      },
      [
        [1, 'hoge'],
        [2, 'fuga']
      ],
      <<~SQL.to_one_line
        SELECT *
          FROM UNNEST(ARRAY<STRUCT<id INT64,
                                   name STRING>>
                      [(1, 'hoge'),
                       (2, 'fuga')])
      SQL
    ],
    "Array column" => [
      {
        id: nil,
        name: nil,
        combination: nil
      },
      [
        [1, 'hoge', [1, 2, 3]],
        [2, 'fuga', [4, 5, 6]]
      ],
      <<~SQL.to_one_line
        SELECT *
          FROM UNNEST(ARRAY<STRUCT<id INT64,
                                   name STRING,
                                   combination ARRAY<INT64>>>
                      [(1, 'hoge', [1, 2, 3]),
                       (2, 'fuga', [4, 5, 6])])
      SQL
    ],
    "FLOAT column" => [
      {
        id: nil,
        ratio: nil
      },
      [
        [1, 1.002],
        [2, 3.14]
      ],
      <<~SQL.to_one_line
        SELECT *
          FROM UNNEST(ARRAY<STRUCT<id INT64,
                                   ratio FLOAT64>>
                      [(1, 1.002),
                       (2, 3.14)])
      SQL
    ],
    "DATE column" => [
      {
        id: nil,
        birthday: nil
      },
      [
        [1, Date.parse('1999-02-11')],
        [2, Date.parse('2000-01-15')]
      ],
      <<~SQL.to_one_line
        SELECT *
          FROM UNNEST(ARRAY<STRUCT<id INT64,
                                   birthday DATE>>
                      [(1, '1999-02-11'),
                       (2, '2000-01-15')])
      SQL
    ],
    "TIMESTAMP column" => [
      {
        id: nil,
        created_at: nil
      },
      [
        [1, DateTime.parse('1999-02-11 12:00:33')],
        [2, DateTime.parse('2000-01-15 17:45:11')]
      ],
      <<~SQL.to_one_line
        SELECT *
          FROM UNNEST(ARRAY<STRUCT<id INT64,
                                   created_at TIMESTAMP>>
                      [(1, '1999-02-11 12:00:33'),
                       (2, '2000-01-15 17:45:11')])
      SQL
    ],
    "BOOL column" => [
      {
        id: nil,
        usable: nil
      },
      [
        [1, true],
        [2, false]
      ],
      <<~SQL.to_one_line
        SELECT *
          FROM UNNEST(ARRAY<STRUCT<id INT64,
                                   usable BOOL>>
                      [(1, true),
                       (2, false)])
      SQL
    ],
    "Single column relation" => [
      { id: nil },
      [
        [1],
        [2],
        [3]
      ],
      <<~SQL.to_one_line
        SELECT id FROM UNNEST(ARRAY<STRUCT<id INT64,
                                           ___dummy STRING>>
                              [(1, NULL),
                               (2, NULL),
                               (3, NULL)])
      SQL
    ],
    "Set fixed types" => [
      {
        id: nil,
        ratio: :FLOAT64
      },
      [
        [1, 1],
        [2, 3.14]
      ],
      <<~SQL.to_one_line
        SELECT *
          FROM UNNEST(ARRAY<STRUCT<id INT64,
                                   ratio FLOAT64>>
                      [(1, 1),
                       (2, 3.14)])
      SQL
    ],
    "Set fixed types as BOOL" => [
      {
        id: nil,
        ratio: :BOOL
      },
      [
        [1, 1],
        [2, false]
      ],
      <<~SQL.to_one_line
        SELECT *
          FROM UNNEST(ARRAY<STRUCT<id INT64,
                                   ratio BOOL>>
                      [(1, true),
                       (2, false)])
      SQL
    ]
  }

  data(NORMAL_TEST_CASES)
  test "Normal" do |(schema, tuples, expected)|
    assert_equal(create_relation_literal(schema, tuples), expected)
  end

  test "Many candidate Error" do
    assert_raise(ReasonlessTypeError) do
      create_relation_literal(
        {
          id: nil,
          name: nil
        },
        [
          [1,   'Taro'],# id is Integer
          ['2', 'Jiro'] # id is String
        ]
      )
    end
  end

  test "Candidate nothing Error" do
    assert_raise(ReasonlessTypeError) do
      create_relation_literal(
        {
          id: nil,
          name: nil
        },
        [
          [1, Object.new], # Object can not apply auto typing
          [2, Object.new]
        ]
      )
    end
  end
end
