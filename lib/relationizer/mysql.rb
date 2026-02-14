require 'json'
require 'bigdecimal'
require 'date'

module Relationizer
  module MySQL
    class ReasonlessTypeError < StandardError; end
    class TypeNotFoundError < StandardError; end

    DEFAULT_TYPES = -> (obj) {
      case obj
      when Integer    then :BIGINT
      when BigDecimal then :'DECIMAL(65,30)'
      when Float      then :DOUBLE
      when String     then :TEXT
      when TrueClass  then :BOOLEAN
      when FalseClass then :BOOLEAN
      when Time       then :DATETIME
      when DateTime   then :DATETIME
      when Date       then :DATE
      else
        nil
      end
    }

    def create_relation_literal(schema, tuples)
      types = fixed_types(schema.values, tuples)
      names = schema.keys

      json_string = to_json_document(names, types, tuples)
      escaped = escape_for_sql(json_string)
      columns_clause = to_columns_clause(names, types)

      %[(SELECT * FROM JSON_TABLE('#{escaped}', "$[*]" COLUMNS(#{columns_clause})) AS t)]
    end

    private

    def fixed_types(schema_values, tuples)
      if tuples.empty?
        raise TypeNotFoundError unless schema_values.all?
        return schema_values
      end

      tuples.transpose.zip(schema_values).map { |values, type|
        next type if type

        values.
          map(&DEFAULT_TYPES).compact.uniq.
          tap(&method(:empty_candidate_check)).
          tap(&method(:many_candidate_check)).
          first
      }
    end

    def many_candidate_check(types)
      raise ReasonlessTypeError.new("Many candidate: #{types.join(', ')}") unless types.one?
    end

    def empty_candidate_check(types)
      raise ReasonlessTypeError.new("Candidate nothing") if types.empty?
    end

    def to_json_value(obj, type)
      return nil if obj.nil?

      case type
      when :'DECIMAL(65,30)'
        obj.is_a?(BigDecimal) ? obj.to_s("F") : obj.to_s
      when :DATE
        obj.strftime('%Y-%m-%d')
      when :DATETIME
        obj.strftime('%Y-%m-%d %H:%M:%S')
      else
        obj
      end
    end

    def to_json_document(names, types, tuples)
      rows = tuples.map { |tuple|
        hash = {}
        names.zip(tuple, types).each do |name, value, type|
          hash[name] = to_json_value(value, type)
        end
        hash
      }
      JSON.generate(rows)
    end

    def escape_for_sql(json_string)
      json_string.gsub('\\', '\\\\\\\\').gsub("'", "''")
    end

    def identifier_quote(name)
      "`#{name}`"
    end

    def to_columns_clause(names, types)
      names.zip(types).map { |name, type|
        %[#{identifier_quote(name)} #{type} PATH "$.#{name}"]
      }.join(', ')
    end
  end
end
