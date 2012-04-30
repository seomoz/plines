require 'spec_helper'
require 'plines'
require 'qless/worker'

describe Plines, :redis do
  before do
    module ::MakeThanksgivingDinner
      extend self

      class BuyGroceries
        include Plines::Step

        def perform
          MakeThanksgivingDinner.add_performed_step :buy_groceries
        end
      end

      class MakeStuffing
        include Plines::Step
        depends_on :BuyGroceries

        def perform
          MakeThanksgivingDinner.add_performed_step :make_stuffing
        end
      end

      class PickupTurkey
        include Plines::Step
        depends_on :BuyGroceries

        def perform
          MakeThanksgivingDinner.add_performed_step :pickup_turkey
        end
      end

      class BrineTurkey
        include Plines::Step
        depends_on :PickupTurkey

        def perform
          MakeThanksgivingDinner.add_performed_step :brine_turkey
        end
      end

      class StuffTurkey
        include Plines::Step
        depends_on :MakeStuffing, :BrineTurkey

        def perform
          MakeThanksgivingDinner.add_performed_step :stuff_turkey
        end
      end

      class BakeTurkey
        include Plines::Step
        depends_on :StuffTurkey

        def perform
          MakeThanksgivingDinner.add_performed_step :bake_turkey
        end
      end

      class PourDrinks
        include Plines::Step
        depends_on :BuyGroceries
        fan_out { |data| data[:drinks].map { |d| { drink: d } } }

        def perform
          MakeThanksgivingDinner.add_poured_drink job_data.drink
          MakeThanksgivingDinner.add_performed_step :pour_drinks
        end
      end

      class SetTable
        include Plines::Step
        depends_on :PourDrinks, :BakeTurkey

        def perform
          MakeThanksgivingDinner.add_performed_step :set_table
        end
      end

      def performed_steps
        Plines.redis.lrange "make_thanksgiving_dinner:performed_steps", 0, -1
      end

      def add_performed_step(step)
        Plines.redis.rpush "make_thanksgiving_dinner:performed_steps", step.to_s
      end

      def poured_drinks
        Plines.redis.lrange "make_thanksgiving_dinner:poured_drinks", 0, -1
      end

      def add_poured_drink(type)
        Plines.redis.rpush "make_thanksgiving_dinner:poured_drinks", type.to_s
      end
    end
  end

  after { Object.send(:remove_const, :MakeThanksgivingDinner) }

  let(:job_reserver) { Qless::JobReservers::Ordered.new([Plines.default_queue]) }
  let(:worker) { Qless::Worker.new(Plines.qless, job_reserver) }

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

  let(:smith_batch) { Plines.most_recent_job_batch_for(family: "Smith") }

  def enqueue_jobs
    Plines.configure do |plines|
      plines.batch_list_key { |d| d[:family] }
    end

    Plines.enqueue_jobs_for(family: "Smith", drinks: %w[ champaign water cider ])

    Plines.most_recent_job_batch_for(family: "Jones").should be_nil
    smith_batch.should have(10).job_jids
    smith_batch.should_not be_complete

    MakeThanksgivingDinner.performed_steps.should eq([])
    MakeThanksgivingDinner.poured_drinks.should eq([])
  end

  it 'enqueues Qless jobs and runs them in the expected order' do
    enqueue_jobs
    worker.work(0)

    steps = MakeThanksgivingDinner.performed_steps
    steps.should have(10).entries

    steps.first.should eq("buy_groceries") # should always be first
    steps.last.should eq("set_table") # should always be last
    steps.count("pour_drinks").should eq(3) # should be in the middle somewhere

    "make_stuffing".should be_before("stuff_turkey").in(steps)
    "brine_turkey".should be_before("stuff_turkey").in(steps)
    "stuff_turkey".should be_before("bake_turkey").in(steps)

    MakeThanksgivingDinner.poured_drinks.should =~ %w[ champaign water cider ]

    smith_batch.should be_complete
  end

  it 'allows a job batch to be cancelled in midstream' do
    enqueue_jobs

    MakeThanksgivingDinner::StuffTurkey.class_eval do
      def perform
        job_batch.cancel!
      end
    end

    Plines.default_queue.length.should eq(1)
    smith_batch.should_not be_cancelled
    worker.work(0)

    Plines.qless.should have_no_failures
    steps = MakeThanksgivingDinner.performed_steps
    steps.should have_at_most(7).entries

    Plines.default_queue.length.should eq(0)
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
end

