#!/usr/bin/env rake
require_relative 'config/setup_load_paths'

require 'qless/tasks'
namespace :qless do
  task :set_redis_url do
    if File.exist?('./config/redis_connection_url.txt')
      ENV['REDIS_URL'] = File.read('./config/redis_connection_url.txt')
    end
  end

  task :setup => :set_redis_url do
    ENV['VVERBOSE'] = '1'
    ENV['QUEUE'] = 'plines'
    ENV['INTERVAL'] = '1.0'
  end

  desc "Start the Qless Web UI"
  task :server => :set_redis_url do
    sh "rackup config/config.ru"
  end
end

require 'rspec/core/rake_task'

RSpec::Core::RakeTask.new(:spec) do |t|
  t.rspec_opts = %w[--profile --format progress]
  t.ruby_opts  = "-Ispec -r./config/setup_load_paths -rsimplecov_setup"
end

if RUBY_ENGINE == 'ruby'
  require 'cane/rake_task'

  desc "Run cane to check quality metrics"
  Cane::RakeTask.new(:quality) do |cane|
    cane.style_glob = "lib/**/*.rb"
    cane.abc_max = 15
    cane.add_threshold 'coverage/coverage_percent.txt', :>=, 100
  end
else
  task :quality do
    # no-op; Cane isn't supported on this interpretter
  end
end

task default: [:spec, :quality]

namespace :ci do
  desc "Run all tests both integrated and in isolation"
  task :spec do
    test_all_script = File.expand_path('../script/test_all', __FILE__)
    sh test_all_script
  end
end


desc "Run CI build"
task ci: %w[ ci:spec quality ]

