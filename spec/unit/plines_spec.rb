require 'spec_helper'
require 'plines'

describe Plines do
  describe ".qless" do
    before      { Plines.instance_variable_set(:@qless, nil) }
    after(:all) { Plines.instance_variable_set(:@qless, nil) }

    it 'returns a memoized Qless::Client instance' do
      Plines.qless.should be_a(Qless::Client)
      Plines.qless.should be(Plines.qless)
    end

    it 'can be overridden' do
      orig_instance = Plines.qless
      new_instance = Qless::Client.new
      Plines.qless = new_instance
      Plines.qless.should be(new_instance)
      Plines.qless.should_not be(orig_instance)
    end
  end

  describe ".default_queue" do
    it "returns the 'plines' queue, memoized" do
      Plines.default_queue.should be_a(Qless::Queue)
      Plines.default_queue.should be(Plines.default_queue)
      Plines.default_queue.name.should eq("plines")
    end
  end

  describe ".start_processing", :redis do
    step_class(:Step1)
    step_class(:Step2)

    before { Plines.default_queue.peek.should be_nil }

    it "enqueues all steps that have no declared dependencies" do
      Plines.start_processing
      enqueued_waiting_job_klass_names(2).should =~ %w[ Step1 Step2 ]
    end

    it "enqueues dependent jobs as dependencies", :pending do
      step_class(:DependsOnStep1) do
        depends_on :Step1
      end

      Plines.start_processing
      enqueued_waiting_job_klass_names(3).should_not include("DependsOnStep1")
    end
  end
end

