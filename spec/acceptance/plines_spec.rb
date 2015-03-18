require 'plines'
require 'qless/worker'
require 'timecop'
require 'redis/list'
require 'qless/test_helpers/worker_helpers'
require 'qless/job_reservers/ordered'

RSpec.configure do |config|
  config.mock_with :rspec do |mock|
    mock.verify_doubled_constant_names = true
  end
end

RSpec.describe Plines, :redis do
  include Qless::WorkerHelpers

  module RedisReconnectMiddleware
    def around_perform(job)
      ::MakeThanksgivingDinner.redis.client.reconnect
      super
    end
  end

  GROCERIES_QUEUE_NAME_FOR = ->(family) { "groceries_for_#{family.downcase}" }

  before do
    module ::MakeThanksgivingDinner
      attr_accessor :redis
      extend Plines::Pipeline
      extend self

      class BuyGroceries
        extend Plines::Step
        depended_on_by_all_steps

        qless_options do |opt, data|
          opt.queue = GROCERIES_QUEUE_NAME_FOR.(data.fetch "family")
          opt.priority = -10
        end

        def perform
          MakeThanksgivingDinner.performed_steps << :buy_groceries
        end
      end

      class MakeStuffing
        extend Plines::Step

        def perform
          MakeThanksgivingDinner.performed_steps << :make_stuffing
        end
      end

      class PickupTurkey
        extend Plines::Step

        def perform
          MakeThanksgivingDinner.performed_steps << :pickup_turkey
        end
      end

      class BrineTurkey
        extend Plines::Step
        depends_on :PickupTurkey

        def perform
          MakeThanksgivingDinner.performed_steps << :brine_turkey
        end
      end

      class StuffTurkey
        extend Plines::Step
        depends_on :MakeStuffing, :BrineTurkey

        def perform
          MakeThanksgivingDinner.performed_steps << :stuff_turkey
        end
      end

      class BakeTurkey
        extend Plines::Step
        depends_on :StuffTurkey

        def perform
          MakeThanksgivingDinner.performed_steps << :bake_turkey
        end
      end

      class PourDrinks
        extend Plines::Step
        fan_out { |data| data["drinks"].map { |d| { "drink" => d, "family" => data["family"] } } }

        def perform
          MakeThanksgivingDinner.poured_drinks << job_data.drink
          MakeThanksgivingDinner.performed_steps << :pour_drinks
        end
      end

      class SetTable
        extend Plines::Step
        depends_on_all_steps

        def perform
          MakeThanksgivingDinner.performed_steps << :set_table
        end
      end

      def performed_steps
        Redis::List.new("make_thanksgiving_dinner:performed_steps", MakeThanksgivingDinner.redis)
      end

      def poured_drinks
        Redis::List.new("make_thanksgiving_dinner:poured_drinks", MakeThanksgivingDinner.redis)
      end

      def unresolved_external_dependencies
        Redis::List.new("make_thanksgiving_dinner:unresolved_external_dependencies", MakeThanksgivingDinner.redis)
      end
    end

    MakeThanksgivingDinner.redis = redis
  end

  after { Object.send(:remove_const, :MakeThanksgivingDinner) }

  def default_queue(client = qless)
    client.queues[Plines::Pipeline::DEFAULT_QUEUE]
  end

  def groceries_queue_for(family, client = qless)
    client.queues[GROCERIES_QUEUE_NAME_FOR.(family)]
  end

  def job_reserver(client = qless)
    queue_names = client.queues.counts.map { |q| q.fetch "name" }
    queue_names.delete Plines::Pipeline::AWAITING_EXTERNAL_DEPENDENCY_QUEUE
    queues = queue_names.map { |name| client.queues[name] }
    Qless::JobReservers::Ordered.new(queues)
  end

  RSpec::Matchers.define :be_before do |expected|
    chain :in do |array|
      @array = array
    end

    match do |actual|
      @array.index(actual) < @array.index(expected)
    end
  end

  RSpec::Matchers.define :have_no_failures do
    match do |actual|
      actual.jobs.failed.size == 0
    end

    failure_message do |actual|
      "expected no failures but got " + failure_details_for(actual)
    end

    def failure_details_for(queue)
      failed_jobs = queue.jobs.failed.keys.inject([]) { |failures, type| failures + actual.jobs.failed(type).fetch('jobs') }
      details = failed_jobs.map do |j|
        [j.failure.fetch('group'), j.failure.fetch('message')].join("\n")
      end.join("\n" + '=' * 80)

      "#{failed_jobs.size} failure(s): \n\n#{details}"
    end
  end

  def plines_temporary_redis_key_ttls
    MakeThanksgivingDinner.redis.keys.reject do |k|
      k.start_with?("make_thanksgiving_dinner") ||
      k.start_with?("ql") ||
      k.end_with?("last_batch_num")
    end.map { |k| MakeThanksgivingDinner.redis.ttl(k) }.uniq
  end

  let(:smith_batch) { MakeThanksgivingDinner.most_recent_job_batch_for("family" => "Smith") }
  let(:qless) { Qless::Client.new(redis: redis) }

  DEFAULT_DRINKS = %w[ champagne water cider ]

  def enqueue_jobs(options = {})
    family = options.fetch("family") { "Smith" }

    batch_data = {
      "family" => family,
      "drinks" => DEFAULT_DRINKS
    }.merge(options)

    MakeThanksgivingDinner.configure do |plines|
      plines.batch_list_key { |d| d["family"] }
      plines.qless_job_options do |job, job_batch|
        { tags: Array(job.data.fetch("family")) + Array(job_batch.create_options["tags"]) }
      end
      plines.qless_client { qless } unless options[:dont_configure_qless_client]
    end

    MakeThanksgivingDinner.enqueue_jobs_for(batch_data, reason: "for testing", tags: "create_tag")

    expect(MakeThanksgivingDinner.most_recent_job_batch_for("family" => family.next)).to be_nil

    batch = MakeThanksgivingDinner.most_recent_job_batch_for("family" => family)
    expect(batch.job_jids.size).to be >= 10
    expect(batch).not_to be_complete
    expect(batch.creation_reason).to eq("for testing")

    unless @already_enqueued_a_batch
      expect(MakeThanksgivingDinner.performed_steps).to eq([])
      expect(MakeThanksgivingDinner.poured_drinks).to eq([])
    end

    @already_enqueued_a_batch = true
    batch
  end

  def should_expire_keys
    expect(plines_temporary_redis_key_ttls).to eq([MakeThanksgivingDinner.configuration.data_ttl_in_seconds])
  end

  def process_work(client = qless)
    worker = new_worker(job_reserver(client))
    drain_worker_queues(worker)
    expect(client).to have_no_failures
  end

  let(:start_time) { Time.new(2012, 5, 1, 8, 30) }
  let(:end_time)   { Time.new(2012, 5, 1, 9, 30) }

  after { Timecop.return }

  def wait_for_seconds(seconds)
    Timecop.freeze(Time.now + seconds)
  end

  shared_examples_for 'plines acceptance tests' do
    it 'enqueues Qless jobs and runs them in the expected order, keeping track of how long the batch took' do
      Timecop.freeze(start_time) { enqueue_jobs }
      groceries_queue = groceries_queue_for("Smith")
      expect(groceries_queue.peek.tags).to contain_exactly("Smith", "create_tag")
      job = groceries_queue.peek
      expect(job.klass.to_s).to eq("MakeThanksgivingDinner::BuyGroceries")
      expect(job.priority).to eq(-10)
      Timecop.freeze(end_time) { process_work }

      steps = MakeThanksgivingDinner.performed_steps.values
      expect(steps.entries.size).to eq(10)

      expect(steps.first).to eq("buy_groceries") # must always be first
      expect(steps.last).to eq("set_table") # must always be last
      expect(steps.count("pour_drinks")).to eq(3) # must be in the middle somewhere

      expect("make_stuffing").to be_before("stuff_turkey").in(steps)
      expect("brine_turkey").to be_before("stuff_turkey").in(steps)
      expect("stuff_turkey").to be_before("bake_turkey").in(steps)

      expect(MakeThanksgivingDinner.poured_drinks.values).to match_array %w[ champagne water cider ]

      expect(smith_batch).to be_complete
      expect(smith_batch.created_at).to eq(start_time)
      expect(smith_batch.completed_at).to eq(end_time)
    end

    it 'allows a job batch to be deleted! midstream' do
      midstream_delete_key_name = 'make_thanksgiving_dinner:midstream_deleted_job_batch'
      enqueue_jobs

      # need to store this for expectations after we've deleted the job batch
      job_jids = smith_batch.job_jids

      MakeThanksgivingDinner.configure do |plines|
        plines.after_job_batch_cancellation do |job_batch|
          MakeThanksgivingDinner.redis.set(midstream_delete_key_name, job_batch)
        end
      end

      MakeThanksgivingDinner::StuffTurkey.class_eval do
        def perform
          job_batch.delete!
          qless_job.retry # so the worker doesn't try to complete it
        end
      end

      expect(groceries_queue_for("Smith").length).to eq(1)
      expect(smith_batch).not_to be_cancelled
      process_work

      steps = MakeThanksgivingDinner.performed_steps
      expect(steps.entries.size).to be <= 7

      expect(default_queue.length).to eq(0)
      plines_keys = MakeThanksgivingDinner.redis.keys('plines:*').reject do |key|
        key.include?('Smith:last_batch_num') # the key should persist
      end
      expect(plines_keys).to be_empty

      cancelled_job_batch = MakeThanksgivingDinner.redis.get(midstream_delete_key_name)
      expect(cancelled_job_batch).to eq(smith_batch.to_s)

      # depending on the order of how jobs get popped, some jobs are cancelled (thus nil)
      # and others are complete. This checked that for all the remaining, non-cancelled jobs
      # they are in the complete state.
      states = job_jids.each_with_object([]) do |jid, states|
        job = smith_batch.qless.jobs[jid]
        states << job.state if job
      end.uniq
      expect(states).to eq(['complete'])
    end

    it 'allows a job batch to be cancelled in midstream' do
      enqueue_jobs

      MakeThanksgivingDinner.configure do |plines|
        plines.after_job_batch_cancellation do |job_batch|
          MakeThanksgivingDinner.redis.set('make_thanksgiving_dinner:midstream_cancelled_job_batch', job_batch)
        end
      end

      MakeThanksgivingDinner::StuffTurkey.class_eval do
        def perform
          job_batch.cancel!(reason: "for testing")
          qless_job.retry # so the worker doesn't try to complete it
        end
      end

      expect(groceries_queue_for("Smith").length).to eq(1)
      expect(smith_batch).not_to be_cancelled
      process_work

      steps = MakeThanksgivingDinner.performed_steps
      expect(steps.entries.size).to be <= 7

      expect(default_queue.length).to eq(0)
      expect(smith_batch).to be_cancelled
      expect(smith_batch.cancelled_at).to be_within(2).of(Time.now)
      expect(smith_batch.cancellation_reason).to eq("for testing")

      cancelled_job_batch = MakeThanksgivingDinner.redis.get('make_thanksgiving_dinner:midstream_cancelled_job_batch')
      expect(cancelled_job_batch).to eq(smith_batch.to_s)

      should_expire_keys
    end

    it 'does not allow a complete job batch to be cancelled' do
      enqueue_jobs

      MakeThanksgivingDinner.configure do |plines|
        plines.after_job_batch_cancellation do |job_batch|
          MakeThanksgivingDinner.redis.set('make_thanksgiving_dinner:midstream_cancelled_job_batch', job_batch)
        end
      end

      expect(groceries_queue_for("Smith").length).to eq(1)
      expect(smith_batch).not_to be_cancelled
      process_work

      steps = MakeThanksgivingDinner.performed_steps
      expect(steps.entries.size).to eq(10)

      expect(smith_batch).to be_complete
      expect {
        smith_batch.cancel
      }.not_to change { smith_batch.cancelled? }.from(false)
    end

    it 'cancels the timeout jobs when the batch is cancelled in midstream' do
      MakeThanksgivingDinner::BakeTurkey.has_external_dependencies do |deps, data|
        deps.add "await_bake_call_proby", wait_up_to: 100000
      end

      MakeThanksgivingDinner::SetTable.has_external_dependencies do |deps, data|
        deps.add "await_set_table_call_proby", wait_up_to: 100000
      end

      enqueue_jobs

      pending_job_keys = MakeThanksgivingDinner.redis.keys('*timeout_job_jids*').size
      expect(pending_job_keys).to eq(2)

      MakeThanksgivingDinner::StuffTurkey.class_eval do
        def perform
          job_batch.cancel!
          qless_job.retry # so the worker doesn't try to complete it
        end
      end

      expect(groceries_queue_for("Smith").length).to eq(1)
      expect(smith_batch).not_to be_cancelled
      process_work

      steps = MakeThanksgivingDinner.performed_steps
      expect(steps.entries.size).to be <= 7

      expect(default_queue.length).to eq(0)
      expect(smith_batch).to be_cancelled

      pending_job_keys = MakeThanksgivingDinner.redis.keys('*timeout_job_jids*').size
      expect(pending_job_keys).to eq(0)

      should_expire_keys
    end

    it "supports external dependencies" do
      MakeThanksgivingDinner::PickupTurkey.class_eval do
        fan_out do |batch_data|
          [:small, :big].map do |s|
            batch_data.merge(size: s)
          end
        end

        has_external_dependencies do |deps, job_data|
          deps.add "await_#{job_data[:size]}_turkey_ready_call"
        end

        def perform
          MakeThanksgivingDinner.performed_steps << "pickup_#{job_data.size}_turkey"
        end
      end

      enqueue_jobs
      process_work

      steps = MakeThanksgivingDinner.performed_steps.values
      expect(steps).not_to include("pickup_small_turkey")
      expect(steps).not_to include("pickup_big_turkey")
      expect(steps.entries.size).to eq(5)

      smith_batch.resolve_external_dependency "await_big_turkey_ready_call"
      process_work
      steps = MakeThanksgivingDinner.performed_steps.values
      expect(steps).not_to include("pickup_small_turkey")
      expect(steps).to include("pickup_big_turkey")

      smith_batch.resolve_external_dependency "await_small_turkey_ready_call"
      process_work

      steps = MakeThanksgivingDinner.performed_steps.values
      expect(steps.entries.size).to eq(11)
      expect(steps).to include("pickup_small_turkey")

      should_expire_keys
    end

    it "can timeout external dependencies" do
      MakeThanksgivingDinner::PickupTurkey.has_external_dependencies do |deps, data|
        deps.add "await_turkey_ready_call", wait_up_to: 300
      end

      MakeThanksgivingDinner::PickupTurkey.class_eval do
        include Module.new {
          def around_perform
            unresolved_external_dependencies.each do |d|
              MakeThanksgivingDinner.unresolved_external_dependencies << d
            end

            super
          end
        }
      end

      Timecop.freeze(Time.now)
      enqueue_jobs
      process_work

      expect(MakeThanksgivingDinner.unresolved_external_dependencies.values).not_to include("await_turkey_ready_call")
      expect(MakeThanksgivingDinner.performed_steps.values).not_to include("pickup_turkey")

      wait_for_seconds(301)
      process_work

      expect(MakeThanksgivingDinner.unresolved_external_dependencies.values).to include("await_turkey_ready_call")
      expect(MakeThanksgivingDinner.performed_steps.values).to include("pickup_turkey")
    end

    it "does not run the timeout jobs if the external deps are resolved before that time" do
      MakeThanksgivingDinner::PickupTurkey.has_external_dependencies do |deps, data|
        deps.add "await_turkey_ready_call", wait_up_to: 100
      end

      Timecop.freeze(Time.now)
      enqueue_jobs
      smith_batch.resolve_external_dependency "await_turkey_ready_call"
      allow(Plines::ExternalDependencyTimeout).to receive(:perform) do
        raise "No timeout jobs should run"
      end

      process_work # finish the batch
      wait_for_seconds 101
      process_work # run any remaining timeouts
    end

    it "supports middleware modules" do
      MakeThanksgivingDinner::PickupTurkey.class_eval do
        include Module.new {
          def around_perform
            MakeThanksgivingDinner.performed_steps << :before_pickup_turkey
            super
            MakeThanksgivingDinner.performed_steps << :after_pickup_turkey
          end
        }
      end

      enqueue_jobs
      process_work

      steps = MakeThanksgivingDinner.performed_steps
      expect(steps.grep(/pickup_turkey/)).to eq(%w[ before_pickup_turkey pickup_turkey after_pickup_turkey ])
    end

    let(:alternate_redis) do
      ::Redis.new(url: redis.id.gsub(/\/(\d+)$/) { |num| "#{num.next}" }).tap do |r|
        r.flushdb # ensure clean slate
      end
    end

    it 'can shard redis usage by the job batch key' do
      alternate_qless = Qless::Client.new(redis: alternate_redis)

      MakeThanksgivingDinner.configure do |c|
        c.qless_client do |family|
          family == "Smith" ? qless : alternate_qless
        end
      end

      enqueue_jobs("family" => "Smith", dont_configure_qless_client: true)
      enqueue_jobs("family" => "Jones", dont_configure_qless_client: true)

      smith_batch = MakeThanksgivingDinner.most_recent_job_batch_for("family" => "Smith")
      jones_batch = MakeThanksgivingDinner.most_recent_job_batch_for("family" => "Jones")

      expect(redis.keys("*Smith*").size).to be > 0
      expect(redis.keys("*Jones*").size).to eq(0)
      expect(alternate_redis.keys("*Jones*").size).to be > 0
      expect(alternate_redis.keys("*Smith*").size).to eq(0)

      process_work(alternate_qless)
      expect(jones_batch).to be_complete
      expect(smith_batch).not_to be_complete

      process_work(qless)
      expect(smith_batch).to be_complete
    end

    it 'keeps track of all batches that timed out a particular external dependency' do
      MakeThanksgivingDinner::PickupTurkey.has_external_dependencies do |deps, data|
        deps.add "await_turkey_ready_call", wait_up_to: 10
      end

      Timecop.freeze(Time.now)
      enqueue_jobs("family" => "Smith", "num" => 1)
      enqueue_jobs("family" => "Smith", "num" => 2)

      wait_for_seconds 11
      process_work # so it times out for 2 of them...

      enqueue_jobs("family" => "Smith", "num" => 3)
      enqueue_jobs("family" => "Smith", "num" => 4)

      batch_list = MakeThanksgivingDinner.job_batch_list_for("family" => "Smith")
      batch_list.each do |batch|
        # Resolve the dependency on some batches, in order to create a batch
        # in each of these 4 states
        #
        # - timed out / not resolved (#1)
        # - timed out / resolved (#2)
        # - not timed out / not resolved (#3)
        # - not timed out / resolved (#4)
        if batch.data["num"].even?
          batch.resolve_external_dependency("await_turkey_ready_call")
        end
      end

      timeouts = batch_list.map(&:timed_out_external_dependencies)
      expect(timeouts).to eq([
        ['await_turkey_ready_call'], ['await_turkey_ready_call'], [], []
      ])

      batches = batch_list.all_with_external_dependency_timeout('await_turkey_ready_call')
      expect(batches.map { |b| b.data.fetch("num") }).to eq([1, 2])
    end

    it 'can spawn a copy of a job batch with overrides' do
      batch = enqueue_jobs("family" => "Smith", "num" => 1)
      spawned = batch.spawn_copy do |options|
        options.data_overrides = { num: 2, copy: true }
        options.reason = "because!"
      end

      process_work

      batch_list = MakeThanksgivingDinner.job_batch_list_for("family" => "Smith")
      batches = batch_list.each.to_a

      expect(batches.map(&:complete?)).to eq([true, true])
      expect(batches.map { |b| b.data["num"] }).to eq([1, 2])
      expect(batches.map { |b| b.data["copy"] }).to eq([nil, true])

      expect(spawned.spawned_from).to eq(batch)
      expect(spawned.creation_reason).to eq("because!")
    end

    it 'can reduce the timeouts when spawning a copy' do
      MakeThanksgivingDinner::PickupTurkey.has_external_dependencies do |deps, data|
        deps.add "await_turkey_ready_call", wait_up_to: 10
      end

      Timecop.freeze(Time.now)
      batch = enqueue_jobs("family" => "Smith")
      wait_for_seconds 5

      spawned = batch.spawn_copy do |options|
        options.timeout_reduction = Time.now - batch.created_at
      end

      wait_for_seconds 6

      process_work
      expect(batch.timed_out_external_dependencies).to eq(['await_turkey_ready_call'])
      expect(spawned.timed_out_external_dependencies).to eq(['await_turkey_ready_call'])

      expect(batch.timeout_reduction).to eq(0)
      expect(spawned.timeout_reduction).to be > 0
    end

    def set_pour_drink_priorities_in_descending_order(batch)
      jobs = batch.qless_jobs.select { |j| j.klass == MakeThanksgivingDinner::PourDrinks }
      jobs.each do |j|
        # Make the first drink's job run last and last first
        j.priority = DEFAULT_DRINKS.index(j.data.fetch "drink")
      end
    end
  end

  context 'single process tests' do
    it_behaves_like 'plines acceptance tests' do
      def new_worker(job_reserver)
        ::Qless::Workers::SerialWorker.new(job_reserver, output: StringIO.new)
      end
    end
  end

  context 'forked tests' do
    it_behaves_like 'plines acceptance tests' do
      def new_worker(job_reserver)
        ::Qless::Workers::ForkingWorker.new(job_reserver, max_startup_interval: 0, output: StringIO.new).tap do |worker|
          worker.extend RedisReconnectMiddleware
        end
      end
    end

    before(:each) do
      pending "This platform does not support forking"
    end unless Process.respond_to?(:fork)
  end
end

