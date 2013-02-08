require 'spec_helper'
require 'plines'
require 'qless/worker'
require 'timecop'
require 'redis/list'

supports_forking = begin
  fork { exit! }
  true
rescue NotImplementedError
  false
end

describe Plines, :redis do
  module RedisReconnectMiddleware
    def around_perform(job)
      ::MakeThanksgivingDinner.redis.client.reconnect
      super
    end
  end

  before do
    module ::MakeThanksgivingDinner
      attr_accessor :redis
      extend Plines::Pipeline
      extend self

      class BuyGroceries
        extend Plines::Step
        depended_on_by_all_steps

        qless_options do |q|
          q.queue = :groceries
          q.priority = -10
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
        fan_out { |data| data[:drinks].map { |d| { drink: d, family: data[:family] } } }

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

  def grocieries_queue(client = qless)
    client.queues[:groceries]
  end

  def job_reserver(client = qless)
    Qless::JobReservers::Ordered.new([default_queue(client), grocieries_queue(client)])
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

    failure_message_for_should do |actual|
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

  let(:smith_batch) { MakeThanksgivingDinner.most_recent_job_batch_for(family: "Smith") }
  let(:qless) { Qless::Client.new(redis: redis) }

  def enqueue_jobs(options = {})
    family = options.fetch(:family) { "Smith" }

    MakeThanksgivingDinner.configure do |plines|
      plines.batch_list_key { |d| d[:family] }
      plines.qless_job_options do |job|
        { tags: Array(job.data.fetch(:family)) }
      end
      plines.qless_client { qless } unless options[:dont_configure_qless_client]
    end

    MakeThanksgivingDinner.enqueue_jobs_for(family: family, drinks: %w[ champaign water cider ])

    expect(MakeThanksgivingDinner.most_recent_job_batch_for(family: family.next)).to be_nil
    expect(smith_batch).to have_at_least(10).job_jids
    expect(smith_batch).not_to be_complete

    expect(MakeThanksgivingDinner.performed_steps).to eq([])
    expect(MakeThanksgivingDinner.poured_drinks).to eq([])
  end

  def should_expire_keys
    expect(plines_temporary_redis_key_ttls).to eq([MakeThanksgivingDinner.configuration.data_ttl_in_seconds])
  end

  def process_work(client = qless)
    worker = self.worker(client)
    worker.extend RedisReconnectMiddleware unless worker.run_as_single_process
    worker.work(0)
    expect(client).to have_no_failures
  end

  let(:start_time) { Time.new(2012, 5, 1, 8, 30) }
  let(:end_time)   { Time.new(2012, 5, 1, 9, 30) }

  shared_examples_for 'plines acceptance tests' do |run_as_single_process|
    define_method :worker do |client = qless|
      Qless::Worker.new(job_reserver(client), run_as_single_process: run_as_single_process)
    end

    it 'enqueues Qless jobs and runs them in the expected order, keeping track of how long the batch took' do
      Timecop.freeze(start_time) { enqueue_jobs }
      expect(grocieries_queue.peek.tags).to eq(["Smith"])
      job = grocieries_queue.peek
      expect(job.klass.to_s).to eq("MakeThanksgivingDinner::BuyGroceries")
      expect(job.priority).to eq(-10)
      Timecop.freeze(end_time) { process_work }

      steps = MakeThanksgivingDinner.performed_steps.values
      expect(steps).to have(10).entries

      expect(steps.first).to eq("buy_groceries") # must always be first
      expect(steps.last).to eq("set_table") # must always be last
      expect(steps.count("pour_drinks")).to eq(3) # must be in the middle somewhere

      expect("make_stuffing").to be_before("stuff_turkey").in(steps)
      expect("brine_turkey").to be_before("stuff_turkey").in(steps)
      expect("stuff_turkey").to be_before("bake_turkey").in(steps)

      expect(MakeThanksgivingDinner.poured_drinks.values).to match_array %w[ champaign water cider ]

      expect(smith_batch).to be_complete
      expect(smith_batch.created_at).to eq(start_time)
      expect(smith_batch.completed_at).to eq(end_time)
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
          job_batch.cancel!
          qless_job.retry # so the worker doesn't try to complete it
        end
      end

      expect(grocieries_queue.length).to eq(1)
      expect(smith_batch).not_to be_cancelled
      process_work

      steps = MakeThanksgivingDinner.performed_steps
      expect(steps).to have_at_most(7).entries

      expect(default_queue.length).to eq(0)
      expect(smith_batch).to be_cancelled

      cancelled_job_batch = MakeThanksgivingDinner.redis.get('make_thanksgiving_dinner:midstream_cancelled_job_batch')
      expect(cancelled_job_batch).to eq(smith_batch.to_s)

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
      expect(steps).to have(5).entries

      smith_batch.resolve_external_dependency "await_big_turkey_ready_call"
      process_work
      steps = MakeThanksgivingDinner.performed_steps.values
      expect(steps).not_to include("pickup_small_turkey")
      expect(steps).to include("pickup_big_turkey")

      smith_batch.resolve_external_dependency "await_small_turkey_ready_call"
      process_work

      steps = MakeThanksgivingDinner.performed_steps.values
      expect(steps).to have(11).entries
      expect(steps).to include("pickup_small_turkey")

      should_expire_keys
    end

    it "can timeout external dependencies" do
      MakeThanksgivingDinner::PickupTurkey.has_external_dependencies do |deps, data|
        deps.add "await_turkey_ready_call", wait_up_to: 0.3
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

      enqueue_jobs
      process_work

      expect(MakeThanksgivingDinner.unresolved_external_dependencies.values).not_to include("await_turkey_ready_call")
      expect(MakeThanksgivingDinner.performed_steps.values).not_to include("pickup_turkey")

      sleep 0.3 # so the timeout occurs
      process_work

      expect(MakeThanksgivingDinner.unresolved_external_dependencies.values).to include("await_turkey_ready_call")
      expect(MakeThanksgivingDinner.performed_steps.values).to include("pickup_turkey")
    end

    it "does not run the timeout jobs if the external deps are resolved before that time" do
      MakeThanksgivingDinner::PickupTurkey.has_external_dependencies do |deps, data|
        deps.add "await_turkey_ready_call", wait_up_to: 0.1
      end

      enqueue_jobs
      smith_batch.resolve_external_dependency "await_turkey_ready_call"
      Plines::ExternalDependencyTimeout.stub(:perform) do
        raise "No timeout jobs should run"
      end

      process_work # finish the batch
      sleep 0.2    # allow the timeout period to pass
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

      enqueue_jobs(family: "Smith", dont_configure_qless_client: true)
      enqueue_jobs(family: "Jones", dont_configure_qless_client: true)

      smith_batch = MakeThanksgivingDinner.most_recent_job_batch_for(family: "Smith")
      jones_batch = MakeThanksgivingDinner.most_recent_job_batch_for(family: "Jones")

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
  end

  context 'single process tests' do
    it_behaves_like 'plines acceptance tests', true
  end

  context 'forked tests' do
    it_behaves_like 'plines acceptance tests', false

    before(:all) do
      pending "This platform does not support forking"
    end unless supports_forking
  end
end

