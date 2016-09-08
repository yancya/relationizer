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
  test "postgresql" do
    schema = { id: nil, name: nil }
    tuples = [[1, 'hoge'], [2, 'fuga']]
    assert_equal(
      create_relation_literal(schema, tuples),
      %Q{SELECT id::INT8, name::TEXT FROM (VALUES('1', 'hoge'), ('2', 'fuga')) AS t("id", "name")}
    )
  end
end
