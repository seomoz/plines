require_relative '../config/setup_load_paths'

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
  config.disable_monkey_patching!
  config.filter_run :focus
  config.run_all_when_everything_filtered = true

  if config.files_to_run.one?
    config.full_backtrace = true
    config.formatter = 'doc' if config.formatters.none?
  end

  config.profile_examples = 10
  config.order = :random
  Kernel.srand config.seed

  config.mock_with :rspec do |mocks|
    mocks.verify_partial_doubles = true
  end

  config.include PlinesSpecHelpers
  config.extend PlinesSpecHelpers::ClassMethods
end

redis_url = if File.exist?('./config/redis_connection_url.txt')
  File.read('./config/redis_connection_url.txt').strip
else
  "redis://localhost:6379/1"
end

redis = nil
RSpec.shared_context "redis", :redis do
  before(:all) do
    redis ||= ::Redis.new(url: redis_url)
  end

  let(:redis) { redis }
  let(:qless) { Qless::Client.new(redis: redis) }

  before(:each) do
    redis.flushdb
    pipeline_module.configuration.qless_client { qless }
  end

  def qless_job_for(jid)
    queue = qless.queues["some-queue"]
    queue.put(Qless::Job, {}, jid: jid)
    queue.pop.tap do |job|
      # ensure we popped the job we think we did.
      expect(job.jid).to eq(jid)
    end
  end
end

RSpec.shared_context "integration helpers" do
  def create_pipeline_with_step(&block)
    step_class(:A) do
      class_eval(&block) if block
      def perform; end
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
  supports_block_expectations

  chain :to_queue do |queue|
    @queue = queue.to_s
  end

  define_method :current_queue do
    qless.jobs[jid].queue_name.to_s
  end

  match do |actual|
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

  match_when_negated do |actual|
    before_queue = current_queue
    actual.call
    after_queue = current_queue

    after_queue == before_queue
  end

  failure_message do
    "expected block to #{description}"
  end

  failure_message_when_negated do
    "expected block not to #{description}"
  end

  description do
    "move job #{jid}#{" to #{@queue}" if @queue}"
  end
end

