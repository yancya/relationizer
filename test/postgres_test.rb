require 'test-unit'
require_relative '../lib/relationizer/postgresql.rb'

class PostgresqlTest < Test::Unit::TestCase
  include Relationizer::Postgresql

  # Create relation as
  #
  #  id | name
  # ----+------
  #   1 | hoge
  #   2 | fuga
  #
  test "Case 1" do
    schema = { id: nil, name: nil }
    tuples = [[1, 'hoge'], [2, 'fuga']]
    assert_equal(
      create_relation_literal(schema, tuples),
      %Q{SELECT id::INT8, name::TEXT FROM (VALUES('1', 'hoge'), ('2', 'fuga')) AS t("id", "name")}
    )
  end

  test "NULL" do
    schema = { id: nil }
    tuples = [[1], [nil]]
    assert_equal(
      create_relation_literal(schema, tuples),
      %Q{SELECT id::INT8 FROM (VALUES('1'), (NULL)) AS t("id")}
    )
  end
end
