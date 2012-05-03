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
        fan_out { |data| data[:drinks].map { |d| { drink: d } } }

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
    end
  end

  after { Object.send(:remove_const, :MakeThanksgivingDinner) }

  let(:job_reserver) { Qless::JobReservers::Ordered.new([MakeThanksgivingDinner.default_queue]) }
  let(:worker) { Qless::Worker.new(MakeThanksgivingDinner.qless, job_reserver) }

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
      actual.failed.size == 0
    end

    failure_message_for_should do |actual|
      "expected no failures but got " + failure_details_for(actual)
    end

    def failure_details_for(queue)
      failed_jobs = queue.failed.keys.inject([]) { |failures, type| failures + actual.failed(type).fetch('jobs') }
      details = failed_jobs.map do |j|
        [j.failure.fetch('group'), j.failure.fetch('message')].join("\n")
      end.join("\n" + '=' * 80)

      "#{failed_jobs.size} failure(s): \n\n#{details}"
    end
  end

  let(:smith_batch) { MakeThanksgivingDinner.most_recent_job_batch_for(family: "Smith") }

  def enqueue_jobs
    MakeThanksgivingDinner.configure do |plines|
      plines.batch_list_key { |d| d[:family] }
    end

    MakeThanksgivingDinner.enqueue_jobs_for(family: "Smith", drinks: %w[ champaign water cider ])

    MakeThanksgivingDinner.most_recent_job_batch_for(family: "Jones").should be_nil
    smith_batch.should have(10).job_jids
    smith_batch.should_not be_complete

    MakeThanksgivingDinner.performed_steps.should eq([])
    MakeThanksgivingDinner.poured_drinks.should eq([])
  end

  let(:start_time) { Time.new(2012, 5, 1, 8, 30) }
  let(:end_time)   { Time.new(2012, 5, 1, 9, 30) }

  it 'enqueues Qless jobs and runs them in the expected order, keeping track of how long the batch took' do
    Timecop.freeze(start_time) { enqueue_jobs }
    Timecop.freeze(end_time) { worker.work(0) }

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

    MakeThanksgivingDinner.default_queue.length.should eq(1)
    smith_batch.should_not be_cancelled
    worker.work(0)

    MakeThanksgivingDinner.qless.should have_no_failures
    steps = MakeThanksgivingDinner.performed_steps
    steps.should have_at_most(7).entries

    MakeThanksgivingDinner.default_queue.length.should eq(0)
    smith_batch.should be_cancelled
  end

  it "supports external dependencies" do
    MakeThanksgivingDinner::PickupTurkey.has_external_dependency :await_turkey_ready_call

    enqueue_jobs
    worker.work(0)

    steps = MakeThanksgivingDinner.performed_steps
    steps.should have(5).entries
    steps.should_not include("pickup_turkey")

    smith_batch.resolve_external_dependency :await_turkey_ready_call
    worker.work(0)
    steps = MakeThanksgivingDinner.performed_steps
    steps.should have(10).entries
    steps.should include("pickup_turkey")
  end

  it "supports middleware modules" do
    MakeThanksgivingDinner::PickupTurkey.class_eval do
      include Module.new {
        def around_perform
          MakeThanksgivingDinner.performed_steps << :before_pickup_turkey
          super { yield }
          MakeThanksgivingDinner.performed_steps << :after_pickup_turkey
        end
      }
    end

    enqueue_jobs
    worker.work(0)

    steps = MakeThanksgivingDinner.performed_steps
    steps.grep(/pickup_turkey/).should eq(%w[ before_pickup_turkey pickup_turkey after_pickup_turkey ])
  end
end

