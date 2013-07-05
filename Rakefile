#!/usr/bin/env rake
require_relative 'config/setup_load_paths'
require 'bundler/gem_helper'
Bundler::GemHelper.install_tasks

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
    cane.abc_max = 16
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


namespace :qless do
  qless_core_dir = "./lib/plines/lua/qless-core"

  desc "Builds the qless-core lua script"
  task :build do
    Dir.chdir(qless_core_dir) do
      sh "make clean && make"
      sh "cp qless-lib.lua .."
    end
  end

  task :update_submodule do
    sh "git submodule update"
  end

  desc "Updates qless-core and rebuilds it"
  task update: [:update_submodule, :build]

  namespace :verify do
    script_file = "lib/plines/lua/qless-lib.lua"

    desc "Verifies the script has no uncommitted changes"
    task :clean do
      git_status = `git status -- #{script_file}`
      unless /working directory clean/.match(git_status)
        raise "#{script_file} is dirty: \n\n#{git_status}\n\n"
      end
    end

    desc "Verifies the script is current"
    task :current do
      require 'digest/md5'
      our_md5 = Digest::MD5.hexdigest(File.read script_file)

      canonical_md5 = Dir.chdir(qless_core_dir) do
        sh "make clean && make"
        Digest::MD5.hexdigest(File.read "qless-lib.lua")
      end

      unless our_md5 == canonical_md5
        raise "The current script is out of date with qless-core's lib script"
      end
    end
  end

  desc "Verifies the committed script is current"
  task verify: %w[ verify:clean verify:current ]
end

