# -*- encoding: utf-8 -*-
require File.expand_path('../lib/plines/version', __FILE__)

Gem::Specification.new do |gem|
  gem.authors       = ["Myron Marston"]
  gem.email         = ["myron.marston@gmail.com"]
  gem.description   = %q{Plines sits on top of Qless and makes it easy to define and run a pipeline of jobs.}
  gem.summary       = %q{Easily express a pipeline of Qless job dependencies}
  gem.homepage      = ""

  gem.files         = %w(README.md LICENSE Gemfile Rakefile) + Dir.glob("lib/**/*.rb") + Dir.glob("lib/**/*.lua")
  gem.name          = "plines"
  gem.require_paths = ["lib"]
  gem.version       = Plines::VERSION

  gem.add_dependency 'redis-objects', '~> 0.6.1'
  gem.add_dependency 'qless'
  gem.add_development_dependency 'rspec', '~> 2.99.0.beta2'
  gem.add_development_dependency 'rspec-fire', '~> 1.2'
  gem.add_development_dependency 'rake', '~> 0.9.2.2'
  gem.add_development_dependency 'simplecov', '~> 0.6.2'
  gem.add_development_dependency 'cane', '~> 2.6'
  gem.add_development_dependency 'timecop', '~> 0.3.5'
end

