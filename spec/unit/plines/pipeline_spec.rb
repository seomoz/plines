require 'spec_helper'
require 'plines/pipeline'
require 'plines/configuration'
require 'plines/job_enqueuer'
require 'plines/dependency_graph'
require 'plines/job'
require 'plines/job_batch_list'
require 'plines/job_batch'

module Plines
  describe Pipeline do
    before do
      mod = Module.new do
        extend Plines::Pipeline
      end
      stub_const("MyPipeline", mod)
    end

    describe ".qless" do
      it 'returns a memoized Qless::Client instance' do
        MyPipeline.qless.should be_a(Qless::Client)
        MyPipeline.qless.should be(MyPipeline.qless)
      end

      it 'can be overridden' do
        orig_instance = MyPipeline.qless
        new_instance = Qless::Client.new
        MyPipeline.qless = new_instance
        MyPipeline.qless.should be(new_instance)
        MyPipeline.qless.should_not be(orig_instance)
      end
    end

    describe ".default_queue" do
      it "returns the 'plines' queue, memoized" do
        MyPipeline.default_queue.should be_a(Qless::Queue)
        MyPipeline.default_queue.should be(MyPipeline.default_queue)
        MyPipeline.default_queue.name.should eq("plines")
      end
    end

    describe ".awaiting_external_dependency_queue" do
      it "returns the 'awaiting_ext_dep' queue, memoized" do
        MyPipeline.awaiting_external_dependency_queue.should be_a(Qless::Queue)
        MyPipeline.awaiting_external_dependency_queue.should be(MyPipeline.awaiting_external_dependency_queue)
        MyPipeline.awaiting_external_dependency_queue.name.should eq("awaiting_ext_dep")
      end
    end

    describe ".configuration" do
      it "returns a memoized Plines::Configuration instance" do
        MyPipeline.configuration.should be_a(Plines::Configuration)
        MyPipeline.configuration.should be(MyPipeline.configuration)
      end
    end

    describe ".configure" do
      it "yields the configuration object" do
        yielded = nil
        MyPipeline.configure { |c| yielded = c }
        yielded.should be(MyPipeline.configuration)
      end
    end

    describe ".redis" do
      it "returns the qless redis" do
        MyPipeline.redis.should be(MyPipeline.qless.redis)
      end
    end

    describe ".most_recent_job_batch_for", :redis do
      it 'returns a job batch using the configured key' do
        MyPipeline.configuration.batch_list_key { |data| data["a"] }
        JobBatchList.new(MyPipeline, "foo").create_new_batch
        batch = MyPipeline.most_recent_job_batch_for("a" => "foo")
        batch.should be_a(JobBatch)
        batch.id.should include("foo")
      end
    end

    describe ".enqueue_jobs_for" do
      it 'enqueues jobs for the given batch data' do
        enqueuer = fire_double("Plines::JobEnqueuer")

        Plines::JobEnqueuer.should_receive(:new) do |graph, job_batch|
          graph.should be_a(Plines::DependencyGraph)
          job_batch.id.should include("foo")
          enqueuer
        end

        enqueuer.should_receive(:enqueue_jobs)

        MyPipeline.configuration.batch_list_key { |data| data["a"] }
        MyPipeline.enqueue_jobs_for("a" => "foo")
      end
    end
  end
end

