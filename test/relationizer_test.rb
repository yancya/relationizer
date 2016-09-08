require 'test-unit'
require_relative '../lib/relationizer/version.rb'

class RelationizerTest < Test::Unit::TestCase
  test "version" do
    assert { Relationizer::VERSION == '0.1.0' }
  end
end
