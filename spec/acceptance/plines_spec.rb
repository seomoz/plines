require 'spec_helper'
require 'plines'
require 'qless/worker'
require 'timecop'

describe Plines, :redis do
  before do
    module ::MakeThanksgivingDinner
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
        @performed_steps ||= Redis::List.new("make_thanksgiving_dinner:performed_steps")
      end

      def poured_drinks
        @poured_drinks ||= Redis::List.new("make_thanksgiving_dinner:poured_drinks")
      end

      def unresolved_external_dependencies
        @unresolved_external_dependencies ||= Redis::List.new("make_thanksgiving_dinner:unresolved_external_dependencies")
      end
    end
  end

  after { Object.send(:remove_const, :MakeThanksgivingDinner) }

  let(:grocieries_queue) { MakeThanksgivingDinner.qless.queues[:groceries] }
  let(:job_reserver) { Qless::JobReservers::Ordered.new([MakeThanksgivingDinner.default_queue, grocieries_queue]) }
  let(:worker) { Qless::Worker.new(MakeThanksgivingDinner.qless, job_reserver) }

  before do
    worker.run_as_single_process = true if RUBY_ENGINE == 'jruby'
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

  def enqueue_jobs
    MakeThanksgivingDinner.configure do |plines|
      plines.batch_list_key { |d| d[:family] }
      plines.qless_job_options do |job|
        { tags: Array(job.data.fetch(:family)) }
      end
    end

    MakeThanksgivingDinner.enqueue_jobs_for(family: "Smith", drinks: %w[ champaign water cider ])

    MakeThanksgivingDinner.most_recent_job_batch_for(family: "Jones").should be_nil
    smith_batch.should have_at_least(10).job_jids
    smith_batch.should_not be_complete

    MakeThanksgivingDinner.performed_steps.should eq([])
    MakeThanksgivingDinner.poured_drinks.should eq([])
  end

  def should_expire_keys
    plines_temporary_redis_key_ttls.should eq([MakeThanksgivingDinner.configuration.data_ttl_in_seconds])
  end

  def process_work
    worker.work(0)
    MakeThanksgivingDinner.qless.should have_no_failures
  end

  let(:start_time) { Time.new(2012, 5, 1, 8, 30) }
  let(:end_time)   { Time.new(2012, 5, 1, 9, 30) }

  it 'enqueues Qless jobs and runs them in the expected order, keeping track of how long the batch took' do
    Timecop.freeze(start_time) { enqueue_jobs }
    grocieries_queue.peek.tags.should eq(["Smith"])
    job = grocieries_queue.peek
    job.klass.to_s.should eq("MakeThanksgivingDinner::BuyGroceries")
    job.priority.should eq(-10)
    Timecop.freeze(end_time) { process_work }

    steps = MakeThanksgivingDinner.performed_steps.values
    steps.should have(10).entries

    steps.first.should eq("buy_groceries") # should always be first
    steps.last.should eq("set_table") # should always be last
    steps.count("pour_drinks").should eq(3) # should be in the middle somewhere

    "make_stuffing".should be_before("stuff_turkey").in(steps)
    "brine_turkey".should be_before("stuff_turkey").in(steps)
    "stuff_turkey".should be_before("bake_turkey").in(steps)

    MakeThanksgivingDinner.poured_drinks.values.should =~ %w[ champaign water cider ]

    smith_batch.should be_complete
    smith_batch.created_at.should eq(start_time)
    smith_batch.completed_at.should eq(end_time)
  end

  it 'allows a job batch to be cancelled in midstream' do
    enqueue_jobs

    MakeThanksgivingDinner::StuffTurkey.class_eval do
      def perform
        job_batch.cancel!
      end
    end

    grocieries_queue.length.should eq(1)
    smith_batch.should_not be_cancelled
    process_work

    steps = MakeThanksgivingDinner.performed_steps
    steps.should have_at_most(7).entries

    MakeThanksgivingDinner.default_queue.length.should eq(0)
    smith_batch.should be_cancelled

    should_expire_keys
  end

  it "supports external dependencies" do
    MakeThanksgivingDinner::PickupTurkey.class_eval do
      fan_out do |batch_data|
        [:small, :big].map do |s|
          batch_data.merge(size: s)
        end
      end

      has_external_dependencies do |job_data|
        "await_#{job_data[:size]}_turkey_ready_call"
      end

      def perform
        MakeThanksgivingDinner.performed_steps << "pickup_#{job_data.size}_turkey"
      end
    end

    enqueue_jobs
    process_work

    steps = MakeThanksgivingDinner.performed_steps.values
    steps.should_not include("pickup_small_turkey")
    steps.should_not include("pickup_big_turkey")
    steps.should have(5).entries

    smith_batch.resolve_external_dependency "await_big_turkey_ready_call"
    process_work
    steps = MakeThanksgivingDinner.performed_steps.values
    steps.should_not include("pickup_small_turkey")
    steps.should include("pickup_big_turkey")

    smith_batch.resolve_external_dependency "await_small_turkey_ready_call"
    process_work

    steps = MakeThanksgivingDinner.performed_steps.values
    steps.should have(11).entries
    steps.should include("pickup_small_turkey")

    should_expire_keys
  end

  it "can timeout external dependencies" do
    MakeThanksgivingDinner::PickupTurkey.has_external_dependencies(wait_up_to: 0.3) do
      "await_turkey_ready_call"
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

    MakeThanksgivingDinner.unresolved_external_dependencies.values.should_not include("await_turkey_ready_call")
    MakeThanksgivingDinner.performed_steps.values.should_not include("pickup_turkey")

    sleep 0.3 # so the timeout occurs
    process_work

    MakeThanksgivingDinner.unresolved_external_dependencies.values.should include("await_turkey_ready_call")
    MakeThanksgivingDinner.performed_steps.values.should include("pickup_turkey")
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
    steps.grep(/pickup_turkey/).should eq(%w[ before_pickup_turkey pickup_turkey after_pickup_turkey ])
  end
end

