# coding: utf-8
lib = File.expand_path('../lib', __FILE__)
$LOAD_PATH.unshift(lib) unless $LOAD_PATH.include?(lib)
require 'relationizer/version'

Gem::Specification.new do |spec|
  spec.name          = "relationizer"
  spec.version       = Relationizer::VERSION
  spec.authors       = ["yancya"]
  spec.email         = ["yancya@upec.jp"]

  spec.summary       = %q{Array<Array> to Relation ppoi String}
  spec.homepage      = "https://github.com/yancya/relationizer"
  spec.license       = "MIT"

  spec.files         = `git ls-files -z`.split("\x0").reject { |f| f.match(%r{^(test|spec|features)/}) }
  spec.bindir        = "exe"
  spec.executables   = spec.files.grep(%r{^exe/}) { |f| File.basename(f) }
  spec.require_paths = ["lib"]

  spec.add_development_dependency "bundler"
  spec.add_development_dependency "rake", ">= 12.3.3"
  spec.add_development_dependency "test-unit", "~> 3.0.0"
end
