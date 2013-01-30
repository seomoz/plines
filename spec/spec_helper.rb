require_relative '../config/setup_load_paths'
unless ENV['TRAVIS']
  require 'debugger'
end
require 'rspec/fire'

RSpec::Matchers.define :have_enqueued_waiting_jobs_for do |*klasses|
  match do |_|
    jobs = Plines.default_queue.peek(klasses.size + 1)
    jobs.map { |j| j.klass.to_s }.should =~ klasses.map(&:to_s)
  end
end

module PlinesSpecHelpers
  def pipeline_module
    @pipeline_module ||= begin
      mod = Module.new do
        def self.name; "P"; end
        extend Plines::Pipeline
      end

      stub_const("P", mod)
      mod
    end
  end

  def step_class(name, &block)
    block ||= Proc.new { }
    klass = Class.new
    pipeline_module.const_set(name, klass)
    klass.class_eval do
      extend Plines::Step
      module_eval(&block)
    end
  end

  def enqueued_waiting_job_klass_names(expected)
    jobs = Plines.default_queue.peek(expected + 1)
    jobs.map { |j| j.klass.to_s }
  end

  module ClassMethods
    def step_class(name, &block)
      before(:each) { step_class(name, &block) }
    end
  end
end

RSpec.configure do |config|
  config.treat_symbols_as_metadata_keys_with_true_values = true
  config.run_all_when_everything_filtered = true
  config.filter_run :f
  config.alias_example_to :fit, :f
  config.include RSpec::Fire
  config.include PlinesSpecHelpers
  config.extend PlinesSpecHelpers::ClassMethods

  config.expect_with :rspec do |expectations|
    expectations.syntax = :expect
  end
end

redis_url = if File.exist?('./config/redis_connection_url.txt')
  File.read('./config/redis_connection_url.txt').strip
else
  "redis://localhost:6379/1"
end

shared_context "redis", :redis do
  redis = nil
  before(:all)  { redis ||= ::Redis.new(url: redis_url) }
  let(:redis) { redis }

  before(:each) do
    redis.flushdb
    pipeline_module.configuration.redis = redis
  end
end

