require_relative '../config/setup_load_paths'
if RUBY_ENGINE == 'ruby' && !ENV['TRAVIS']
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

redis = nil
shared_context "redis", :redis do
  before(:all) do
    redis ||= ::Redis.new(url: redis_url)
  end

  let(:redis) { redis }
  let(:qless) { Qless::Client.new(redis: redis) }

  before(:each) do
    redis.flushdb
    pipeline_module.configuration.qless_client { qless } if defined?(Plines::Pipeline)
  end
end

shared_context "integration helpers" do
  def create_pipeline_with_step(&block)
    step_class(:A) do
      class_eval(&block) if block
      def perform
        qless_job.complete
      end
    end

    P.configure do |config|
      config.batch_list_key { |hash| hash[:id] }
    end
  end

  def enqueue_batch
    P.enqueue_jobs_for(id: 1)
    P.most_recent_job_batch_for(id: 1).tap do |batch|
      expect(batch.job_jids.size).to eq(1)
    end
  end
end

RSpec::Matchers.define :move_job do |jid|
  chain :to_queue do |queue|
    @queue = queue.to_s
  end

  define_method :current_queue do
    qless.jobs[jid].queue_name.to_s
  end

  match_for_should do |actual|
    before_queue = current_queue
    actual.call
    after_queue = current_queue

    if @queue
      after_queue != before_queue &&
      after_queue == @queue
    else
      after_queue != before_queue
    end
  end

  match_for_should_not do |actual|
    before_queue = current_queue
    actual.call
    after_queue = current_queue

    after_queue == before_queue
  end

  failure_message_for_should do
    "expected block to #{description}"
  end

  failure_message_for_should_not do
    "expected block not to #{description}"
  end

  description do
    "move job #{jid}#{" to #{@queue}" if @queue}"
  end
end

