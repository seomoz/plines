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

  describe ".start_processing" do
    it 'builds the dependency graph and enqueues jobs' do
      graph_class = fire_replaced_class_double("Plines::DependencyGraph")
      enqueuer_class = fire_replaced_class_double("Plines::JobEnqueuer")
      enqueuer = fire_double("Plines::JobEnqueuer")

      graph_class.should_receive(:build_for).with("job" => "data") { :the_graph }
      enqueuer_class.should_receive(:new).with(:the_graph) { enqueuer }
      enqueuer.should_receive(:enqueue_jobs)

      Plines.start_processing("job" => "data")
    end
  end
end

