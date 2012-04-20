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

