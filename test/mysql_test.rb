require 'test-unit'
require 'time'
require 'bigdecimal'
require_relative '../lib/relationizer/mysql'
require_relative './to_one_line.rb'

class MySQLTest < Test::Unit::TestCase
  include Relationizer::MySQL
  using ToOneLine

  NORMAL_TEST_CASES = {
    "BIGINT and TEXT" => [
      { id: nil, name: nil },
      [[1, 'hoge'], [2, 'fuga']],
      <<~SQL.to_one_line
        (SELECT * FROM JSON_TABLE('[{"id":1,"name":"hoge"},{"id":2,"name":"fuga"}]',
        "$[*]" COLUMNS(`id` BIGINT PATH "$.id", `name` TEXT PATH "$.name")) AS t)
      SQL
    ],
    "NULL handling" => [
      { id: nil, name: nil },
      [[1, 'hoge'], [nil, nil]],
      <<~SQL.to_one_line
        (SELECT * FROM JSON_TABLE('[{"id":1,"name":"hoge"},{"id":null,"name":null}]',
        "$[*]" COLUMNS(`id` BIGINT PATH "$.id", `name` TEXT PATH "$.name")) AS t)
      SQL
    ],
    "DOUBLE (Float)" => [
      { ratio: nil },
      [[0.0005], [3.14]],
      <<~SQL.to_one_line
        (SELECT * FROM JSON_TABLE('[{"ratio":0.0005},{"ratio":3.14}]',
        "$[*]" COLUMNS(`ratio` DOUBLE PATH "$.ratio")) AS t)
      SQL
    ],
    "BOOLEAN" => [
      { flag: nil },
      [[true], [false]],
      <<~SQL.to_one_line
        (SELECT * FROM JSON_TABLE('[{"flag":true},{"flag":false}]',
        "$[*]" COLUMNS(`flag` BOOLEAN PATH "$.flag")) AS t)
      SQL
    ],
    "DATE" => [
      { d: nil },
      [[Date.parse('2017-04-25')]],
      <<~SQL.to_one_line
        (SELECT * FROM JSON_TABLE('[{"d":"2017-04-25"}]',
        "$[*]" COLUMNS(`d` DATE PATH "$.d")) AS t)
      SQL
    ],
    "DATETIME (Time)" => [
      { ts: nil },
      [[Time.parse('2017-04-25 16:36:00')]],
      <<~SQL.to_one_line
        (SELECT * FROM JSON_TABLE('[{"ts":"2017-04-25 16:36:00"}]',
        "$[*]" COLUMNS(`ts` DATETIME PATH "$.ts")) AS t)
      SQL
    ],
    "DATETIME (DateTime)" => [
      { ts: nil },
      [[DateTime.parse('2017-04-25 16:36:00')]],
      <<~SQL.to_one_line
        (SELECT * FROM JSON_TABLE('[{"ts":"2017-04-25 16:36:00"}]',
        "$[*]" COLUMNS(`ts` DATETIME PATH "$.ts")) AS t)
      SQL
    ],
    "DECIMAL (BigDecimal)" => [
      { amount: nil },
      [[BigDecimal('123.456')]],
      <<~SQL.to_one_line
        (SELECT * FROM JSON_TABLE('[{"amount":"123.456"}]',
        "$[*]" COLUMNS(`amount` DECIMAL(65,30) PATH "$.amount")) AS t)
      SQL
    ],
    "Manual type specification" => [
      { id: nil, ratio: :'DOUBLE' },
      [[1, 1], [2, 3]],
      <<~SQL.to_one_line
        (SELECT * FROM JSON_TABLE('[{"id":1,"ratio":1},{"id":2,"ratio":3}]',
        "$[*]" COLUMNS(`id` BIGINT PATH "$.id", `ratio` DOUBLE PATH "$.ratio")) AS t)
      SQL
    ],
    "Empty tuples with types" => [
      { id: :BIGINT, name: :TEXT },
      [],
      <<~SQL.to_one_line
        (SELECT * FROM JSON_TABLE('[]',
        "$[*]" COLUMNS(`id` BIGINT PATH "$.id", `name` TEXT PATH "$.name")) AS t)
      SQL
    ],
    "Single column (no dummy needed)" => [
      { id: nil },
      [[1], [2], [3]],
      <<~SQL.to_one_line
        (SELECT * FROM JSON_TABLE('[{"id":1},{"id":2},{"id":3}]',
        "$[*]" COLUMNS(`id` BIGINT PATH "$.id")) AS t)
      SQL
    ],
    "String containing single quote" => [
      { name: nil },
      [["it's"], ["O'Reilly"]],
      <<~SQL.to_one_line
        (SELECT * FROM JSON_TABLE('[{"name":"it''s"},{"name":"O''Reilly"}]',
        "$[*]" COLUMNS(`name` TEXT PATH "$.name")) AS t)
      SQL
    ],
  }

  data(NORMAL_TEST_CASES)
  test "Normal" do |(schema, tuples, expected)|
    assert_equal(expected, create_relation_literal(schema, tuples))
  end

  test "Many candidate Error" do
    assert_raise(Relationizer::MySQL::ReasonlessTypeError) do
      create_relation_literal(
        { id: nil },
        [[1], ['2']]
      )
    end
  end

  test "Candidate nothing Error" do
    assert_raise(Relationizer::MySQL::ReasonlessTypeError) do
      create_relation_literal(
        { id: nil },
        [[Object.new], [Object.new]]
      )
    end
  end

  test "Type not found Error (empty tuples without types)" do
    assert_raise(Relationizer::MySQL::TypeNotFoundError) do
      create_relation_literal(
        { id: nil, name: nil },
        []
      )
    end
  end
end
