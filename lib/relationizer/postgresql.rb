require 'bigdecimal'
require 'date'

module Relationizer
  module Postgresql
    class ReasonlessTypeError < StandardError; end

    DEFAULT_TYPES = {
      Integer    => :int8,
      Fixnum     => :int8,
      Bignum     => :decimal,
      BigDecimal => :decimal,
      Float      => :float8,
      String     => :text,
      TrueClass  => :boolean,
      FalseClass => :boolean,
      Date       => :date,
      Time       => :timestamptz
    }

    def create_relation_literal(schema, tuples)
      _select_exp = select_exp(schema, tuples)

      tuples_exp = tuples.map { |tuple|
        tuple.
          map(&method(:to_text_literal)).
          join(", ").
          tap { |t| break "(#{t})" }
      }.join(", ")

      schema_exp = schema.keys.map(&method(:identifer_quote)).join(", ")

      "SELECT #{_select_exp} FROM (VALUES#{tuples_exp}) AS t(#{schema_exp})"
    end

    private

    def select_exp(schema, tuples)
      tuples.transpose.zip(schema.to_a).map { |(values, (name, type))|
        next "#{name}::#{type.to_s.upcase}" if type

        values.
          map(&:class).uniq.
          map(&DEFAULT_TYPES).compact.uniq.
          tap(&method(:many_candidate_check)).
          tap(&method(:empty_candidate_check)).
          first.
          to_s.upcase.
          tap { |fixed_type| break "#{name}::#{fixed_type}" }
      }.join(", ")
    end

    def many_candidate_check(types)
      raise ReasonlessTypeError.new("Many candidate: #{types.join(', ')}") unless types.one?
    end

    def empty_candidate_check(types)
      raise ReasonlessTypeError.new("Candidate nothing") if types.empty?
    end

    def identifer_quote(w)
      %Q{"#{w.to_s.gsub(/"/, '""')}"}
    end

    def to_text_literal(obj)
      obj.to_s.gsub(/'/, "''").tap do |s|
        break "'#{s}'"
      end
    end
  end
end
