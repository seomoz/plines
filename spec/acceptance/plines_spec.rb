require 'spec_helper'
require 'plines'
require 'qless/worker'

describe Plines, :redis do
  step_module(:MakeThanksgivingDinner) do
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

    class BrineTurkey
      include Plines::Step
      depends_on :BuyGroceries

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
        MakeThanksgivingDinner.add_poured_drink job_data["drink"]
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

  it 'enqueues Qless jobs and runs them in the expected order' do
    Plines.configure do |plines|
      plines.batch_group_key { |d| d[:family] }
    end

    Plines.enqueue_jobs_for(family: "Smith", drinks: %w[ champaign water cider ])

    Plines.job_batch_for(family: "Jones").should have(0).job_jids
    Plines.job_batch_for(family: "Smith").should have(9).job_jids

    MakeThanksgivingDinner.performed_steps.should eq([])
    MakeThanksgivingDinner.poured_drinks.should eq([])

    worker.work(0)

    steps = MakeThanksgivingDinner.performed_steps
    steps.should have(9).entries

    steps.first.should eq("buy_groceries") # should always be first
    steps.last.should eq("set_table") # should always be last
    steps.count("pour_drinks").should eq(3) # should be in the middle somewhere

    "make_stuffing".should be_before("stuff_turkey").in(steps)
    "brine_turkey".should be_before("stuff_turkey").in(steps)
    "stuff_turkey".should be_before("bake_turkey").in(steps)

    MakeThanksgivingDinner.poured_drinks.should =~ %w[ champaign water cider ]
  end
end
