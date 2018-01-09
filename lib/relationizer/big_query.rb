require 'bigdecimal'
require 'date'

module Relationizer
  module BigQuery
    class ReasonlessTypeError < StandardError; end

    KNOWN_TYPES = [:INT64, :FLOAT64, :STRING, :BOOL, :TIMESTAMP, :DATE]

    DEFAULT_TYPES = -> (obj) {
      case obj
      when Integer    then :INT64
      when BigDecimal then :FLOAT64
      when Float      then :FLOAT64
      when String     then :STRING
      when TrueClass  then :BOOL
      when FalseClass then :BOOL
      when Time       then :TIMESTAMP
      when DateTime   then :TIMESTAMP
      when Date       then :DATE
      when Array      then :ARRAY
      else
        nil
      end
    }

    def create_relation_literal(schema, tuples)
      types = fixed_types(schema.values, tuples)

      _types_exp = types_exp(schema.keys, types)

      tuples_exp = tuples.map { |tuple|
        tuple.zip(types).
          map { |(col, type)| to_literal(col, type) }.
          join(", ").
          tap { |t| break "(#{t}#{', NULL' if tuple.length == 1})" }
      }.join(", ").tap { |t| break "[#{t}]"}

      select_exp = if schema.one?
                      "#{schema.keys.first}"
                    else
                      '*'
                    end

      "SELECT #{select_exp} FROM UNNEST(#{_types_exp}#{tuples_exp})"
    end

    private

    def array_type(array)
      classes = array.compact.map(&:class).uniq

      unless classes.length == 1
        raise ReasonlessTypeError.new("Ambiguous type of element in array: #{classes}")
      end

      DEFAULT_TYPES[array.first] || :STRING
    end

    def types_exp(names, types)
      case names.length
      when 1
        %Q{ARRAY<STRUCT<#{names.first} #{types.first}, ___dummy STRING>>}
      else
        %Q{ARRAY<STRUCT<#{names.zip(types).map { |(name, type)| "#{name} #{type}" }.join(", ")}>>}
      end
    end

    def many_candidate_check(types)
      raise ReasonlessTypeError.new("Many candidate: #{types.join(', ')}") unless types.one?
    end

    def empty_candidate_check(types)
      raise ReasonlessTypeError.new("Candidate nothing") if types.empty?
    end

    def fixed_types(schema, tuples)
      tuples.transpose.zip(schema.to_a).map { |values, type|
        next type if type

        if values.map { |o| o.is_a?(Array) }.all?
          types = values.
                    map(&method(:array_type)).uniq.
                    tap(&method(:empty_candidate_check)).
                    tap(&method(:many_candidate_check))

          next "ARRAY<#{types.first}>".to_sym
        end

        values.
          map(&DEFAULT_TYPES).compact.uniq.
          tap(&method(:empty_candidate_check)).
          tap(&method(:many_candidate_check)).
          first || :STRING
      }
    end

    def to_literal(obj, type)
      return "NULL" if obj.nil?

      case type
      when :ARRAY
        t = array_type(obj)
        obj.map { |e| to_literal(e, t) }.join(', ').tap { |s| break "[#{s}]"}
      when /^ARRAY\<.+\>$/
        t = /^ARRAY\<(.+)\>$/.match(type).to_a&.dig(1).to_sym
        raise "Unknown type: #{t}" unless KNOWN_TYPES.include?(t)
        obj.map { |e| to_literal(e, t) }.join(', ').tap { |s| break "[#{s}]"}
      when :TIMESTAMP
        %Q{'#{obj.strftime('%Y-%m-%d %H:%M:%S')}'}
      when :STRING, :DATE
        obj.to_s.gsub(/'/, "\'").tap do |s|
          break "'#{s}'"
        end
      when :BOOL
        case obj
        when TrueClass, FalseClass
          obj
        else
          !!obj
        end
      when :FLOAT64
        case obj
        when Float::INFINITY
          "CAST('inf' AS FLOAT64)"
        when -Float::INFINITY
          "CAST('-inf' AS FLOAT64)"
        else
          obj
        end
      when :INT64
        obj.to_s
      else
        raise "Unknown type: #{type}"
      end
    end
  end
end
