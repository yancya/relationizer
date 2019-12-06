require 'test-unit'
require 'time'
require_relative '../lib/relationizer/postgresql.rb'

class PostgresqlTest < Test::Unit::TestCase
  include Relationizer::Postgresql

  TEST_CASES = {
    "NULL" => [
      { id: nil },
      [[1], [nil]],
      %Q{SELECT id::INT8 FROM (VALUES('1'), (NULL)) AS t("id")}
    ],
    "INT and TEXT" => [
      { id: nil, name: nil },
      [[1, 'hoge'], [2, 'fuga']],
      %Q{SELECT id::INT8, name::TEXT FROM (VALUES('1', 'hoge'), ('2', 'fuga')) AS t("id", "name")}
    ],
    "Bignum" => [
      { id: nil },
      [[2_147_483_648]], # 2_147_483_648.bit_length #=> 32
      %Q{SELECT id::INT8 FROM (VALUES('2147483648')) AS t("id")}
    ],
    "Float" => [
      { f: nil },
      [[0.0005]],
      %Q{SELECT f::FLOAT8 FROM (VALUES('0.0005')) AS t("f")}
    ],
    "Bool" => [
      { b: nil },
      [[true], [false]],
      %Q{SELECT b::BOOLEAN FROM (VALUES('true'), ('false')) AS t("b")}
    ],
    "Date" => [
      { date: nil },
      [[Date.parse('2017-04-25')]],
      %Q{SELECT date::DATE FROM (VALUES('2017-04-25')) AS t("date")}
    ],
    "Time" => [
      { time: nil },
      [[Time.parse('2017-04-25 16:36:00 00:00:00 +0900')]],
      %Q{SELECT time::TIMESTAMPTZ FROM (VALUES('2017-04-25 16:36:00 +0900')) AS t("time")}
    ],
    "DateTime" => [
      { time: nil },
      [[DateTime.parse('2017-04-25 16:36:00 00:00:00 +0900')]],
      %Q{SELECT time::TIMESTAMPTZ FROM (VALUES('2017-04-25T16:36:00+00:00')) AS t("time")}
    ]
  }

  data(TEST_CASES)
  test "Auto cast test" do |(schema, tuples, expected)|
    assert_equal(create_relation_literal(schema, tuples), expected)
  end
end
