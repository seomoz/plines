# -*- encoding: utf-8 -*-
require File.expand_path('../lib/plines/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Myron Marston"]
  gem.email         = ["myron.marston@gmail.com"]
  gem.description   = %q{Builds the Moz shard creation pipeline from individually defined steps.}
  gem.summary       = %q{Moz shard creation pipeline builder.}
  gem.homepage      = ""

  gem.files         = %w(README.md LICENSE Gemfile Rakefile) + Dir.glob("lib/**/*.rb")
  gem.name          = "plines"
  gem.require_paths = ["lib"]
  gem.version       = Plines::VERSION

  gem.add_development_dependency 'rspec', '~> 2.9'
  gem.add_development_dependency 'rspec-fire', '~> 0.4'
  gem.add_development_dependency 'rake', '~> 0.9.2.2'
  gem.add_development_dependency 'debugger', '~> 1.1.1'
end
