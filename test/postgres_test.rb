require 'test-unit'
require 'time'
require_relative '../lib/relationizer/postgresql'

class PostgresqlTest < Test::Unit::TestCase
  include Relationizer::Postgresql

  TEST_CASES = {
    "NULL" => [
      { id: nil },
      [[1], [nil]],
      %Q{SELECT "id"::INT8 FROM (VALUES('1'), (NULL)) AS t("id")}
    ],
    "INT and TEXT" => [
      { id: nil, name: nil },
      [[1, 'hoge'], [2, 'fuga']],
      %Q{SELECT "id"::INT8, "name"::TEXT FROM (VALUES('1', 'hoge'), ('2', 'fuga')) AS t("id", "name")}
    ],
    "Bignum" => [
      { id: nil },
      [[2_147_483_648]], # 2_147_483_648.bit_length #=> 32
      %Q{SELECT "id"::INT8 FROM (VALUES('2147483648')) AS t("id")}
    ],
    "Float" => [
      { f: nil },
      [[0.0005]],
      %Q{SELECT "f"::FLOAT8 FROM (VALUES('0.0005')) AS t("f")}
    ],
    "Bool" => [
      { b: nil },
      [[true], [false]],
      %Q{SELECT "b"::BOOLEAN FROM (VALUES('true'), ('false')) AS t("b")}
    ],
    "Date" => [
      { date: nil },
      [[Date.parse('2017-04-25')]],
      %Q{SELECT "date"::DATE FROM (VALUES('2017-04-25')) AS t("date")}
    ],
    "Time" => [
      { time: nil },
      [[Time.parse('2017-04-25 16:36:00+09:00')]],
      %Q{SELECT "time"::TIMESTAMPTZ FROM (VALUES('2017-04-25 16:36:00 +0900')) AS t("time")}
    ],
    "DateTime" => [
      { time: nil },
      [[DateTime.parse('2017-04-25 16:36:00+09:00')]],
      %Q{SELECT "time"::TIMESTAMPTZ FROM (VALUES('2017-04-25T16:36:00+09:00')) AS t("time")}
    ]
  }

  data(TEST_CASES)
  test "Auto cast test" do |(schema, tuples, expected)|
    assert_equal(create_relation_literal(schema, tuples), expected)
  end

  test "Empty tuples with types" do
    assert_equal(
      %Q{SELECT "id"::INT8, "name"::TEXT FROM (VALUES(NULL, NULL)) AS t("id", "name") WHERE FALSE},
      create_relation_literal({ id: :INT8, name: :TEXT }, [])
    )
  end

  test "Empty tuples with a single column" do
    assert_equal(
      %Q{SELECT "id"::INT8 FROM (VALUES(NULL)) AS t("id") WHERE FALSE},
      create_relation_literal({ id: :INT8 }, [])
    )
  end

  test "Type not found Error (empty tuples without types)" do
    assert_raise(Relationizer::Postgresql::TypeNotFoundError) do
      create_relation_literal({ id: nil, name: nil }, [])
    end
  end

  test "Many candidate Error" do
    assert_raise(Relationizer::Postgresql::ReasonlessTypeError) do
      create_relation_literal({ id: nil }, [[1], ['2']])
    end
  end

  test "Candidate nothing Error" do
    assert_raise(Relationizer::Postgresql::ReasonlessTypeError) do
      create_relation_literal({ id: nil }, [[Object.new], [Object.new]])
    end
  end

  test "Column name containing a double quote" do
    assert_equal(
      %Q{SELECT "i""d"::INT8 FROM (VALUES('1')) AS t("i""d")},
      create_relation_literal({ %Q{i"d} => nil }, [[1]])
    )
  end

  test "Column name containing a double quote with empty tuples" do
    assert_equal(
      %Q{SELECT "i""d"::INT8 FROM (VALUES(NULL)) AS t("i""d") WHERE FALSE},
      create_relation_literal({ %Q{i"d} => :INT8 }, [])
    )
  end
end
