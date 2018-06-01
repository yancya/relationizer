require 'json'
require 'bigdecimal'
require 'date'

module Relationizer
  module MySQL
    class ReasonlessTypeError < StandardError; end
    # @param schema Hash
    # @param tuples Array<Array>
    # for example
    # create_relation_literal({a: :INT}, [[1]])
    # => "(SELECT * FROM JSON_TABLE('[{"a":1}]', "$[*]" COLUMNS(a INT PATH "$.a")) AS t)"
    def create_relation_literal(schema, tuples)
      json = to_json(schema, tuples)
      schema_for_table = to_schema_for_table(schema, tuples)
      "(SELECT * FROM JSON_TABLE('#{json}', #{schema_for_table}) AS t)"
    end

    private

    def to_json(schema, tuples)
      tuples.map { |tuple|
        schema.keys.zip(tuple).to_h
      }.to_json
    end

    def to_schema_for_table(schema, tuples)
      columns = schema.map { |name, type|
        %[#{name} #{type} PATH "$.#{name}"]
      }.join(',')
      %["$[*]" COLUMNS(#{columns})]
    end
  end
end
