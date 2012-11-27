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

    describe ".job_batch_list_for", :redis do
      it 'returns a job batch list object' do
        MyPipeline.configuration.batch_list_key { |data| data["a"] }
        batch = MyPipeline.job_batch_list_for("a" => "foo")
        batch.should be_a(JobBatchList)
        batch.key.should eq("foo")
      end
    end

    describe ".enqueue_jobs_for" do
      it 'enqueues jobs for the given batch data' do
        qless_job_block_called = false
        MyPipeline.configuration.qless_job_options { |job| qless_job_block_called = true; {} }
        enqueuer = fire_double("Plines::JobEnqueuer")

        Plines::JobEnqueuer.should_receive(:new) do |graph, job_batch, &block|
          graph.should be_a(Plines::DependencyGraph)
          job_batch.id.should include("foo")
          block.call(stub)
          qless_job_block_called.should be_true
          enqueuer
        end

        enqueuer.should_receive(:enqueue_jobs)

        MyPipeline.configuration.batch_list_key { |data| data["a"] }
        MyPipeline.enqueue_jobs_for("a" => "foo")
      end

      it 'returns the job batch' do
        MyPipeline.configuration.batch_list_key { |data| data["a"] }
        MyPipeline.enqueue_jobs_for("a" => "foo").should be_a(JobBatch)
      end
    end

    describe ".root_dependency" do
      it 'returns a null object implementation by default' do
        MyPipeline.root_dependency.jobs_for("some" => "data").should eq([])
      end

      it 'returns the assigned root dependency' do
        MyPipeline.root_dependency = :my_root_dependency
        MyPipeline.root_dependency.should be(:my_root_dependency)
      end

      it 'cannot be re-assigned (since there cannot be multiple root dependencies)' do
        MyPipeline.root_dependency = :A
        expect {
          MyPipeline.root_dependency = :B
        }.to raise_error(Pipeline::RootDependencyAlreadySetError)
      end
    end

    describe ".set_expiration_on", :redis do
      it 'sets the configured expiration on the given redis key' do
        MyPipeline.redis.set "foo", "bar"
        MyPipeline.redis.ttl("foo").should eq(-1) # -1 means no TTL is set

        MyPipeline.configuration.data_ttl_in_seconds = 3
        MyPipeline.set_expiration_on("foo")
        MyPipeline.redis.ttl("foo").should eq(3)
      end

      it 'can expire multiple keys' do
        MyPipeline.redis.set "foo", "a"
        MyPipeline.redis.set "bar", "a"

        MyPipeline.configuration.data_ttl_in_seconds = 3
        MyPipeline.set_expiration_on("foo", "bar")
        MyPipeline.redis.ttl("foo").should eq(3)
        MyPipeline.redis.ttl("bar").should eq(3)
      end
    end
  end
end

