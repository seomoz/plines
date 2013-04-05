require 'spec_helper'
require 'plines/pipeline'
require 'plines/configuration'
require 'plines/job_enqueuer'
require 'plines/dependency_graph'
require 'plines/job'
require 'plines/job_batch_list'
require 'plines/job_batch'
require 'plines/enqueued_job'
require 'timecop'

module Plines
  describe Pipeline, :redis do
    before do
      qless = self.qless
      mod = Module.new do
        extend Plines::Pipeline
        configure do |c|
          c.qless_client { qless }
        end
      end
      stub_const("MyPipeline", mod)
    end

    describe ".configuration" do
      it "returns a memoized Plines::Configuration instance" do
        expect(MyPipeline.configuration).to be_a(Plines::Configuration)
        expect(MyPipeline.configuration).to be(MyPipeline.configuration)
      end
    end

    describe ".configure" do
      it "yields the configuration object" do
        yielded = nil
        MyPipeline.configure { |c| yielded = c }
        expect(yielded).to be(MyPipeline.configuration)
      end
    end

    describe ".most_recent_job_batch_for", :redis do
      it 'returns a job batch using the configured key' do
        MyPipeline.configuration.batch_list_key { |data| data["a"] }
        JobBatchList.new(MyPipeline, "foo").create_new_batch({})
        batch = MyPipeline.most_recent_job_batch_for("a" => "foo")
        expect(batch).to be_a(JobBatch)
        expect(batch.id).to include("foo")
      end
    end

    describe ".job_batch_list_for", :redis do
      it 'returns a job batch list object' do
        MyPipeline.configuration.batch_list_key { |data| data["a"] }
        batch = MyPipeline.job_batch_list_for("a" => "foo")
        expect(batch).to be_a(JobBatchList)
        expect(batch.key).to eq("foo")
      end
    end

    describe ".enqueue_jobs_for" do
      it 'enqueues jobs for the given batch data' do
        qless_job_block_called = false
        MyPipeline.configuration.qless_job_options { |job| qless_job_block_called = true; {} }
        enqueuer = fire_double("Plines::JobEnqueuer")

        Plines::JobEnqueuer.should_receive(:new) do |graph, job_batch, &block|
          expect(graph).to be_a(Plines::DependencyGraph)
          expect(job_batch.id).to include("foo")
          block.call(stub)
          expect(qless_job_block_called).to be_true
          enqueuer
        end

        enqueuer.should_receive(:enqueue_jobs)

        MyPipeline.configuration.batch_list_key { |data| data["a"] }
        MyPipeline.enqueue_jobs_for("a" => "foo")
      end

      it 'returns the job batch' do
        MyPipeline.configuration.batch_list_key { |data| data["a"] }
        batch = MyPipeline.enqueue_jobs_for("a" => "foo")
        expect(batch).to be_a(JobBatch)
        expect(batch.data).to eq("a" => "foo")
      end
    end

    shared_examples_for 'a dependency boundary' do |attr|
      it 'returns a null object implementation by default' do
        expect(MyPipeline.send(attr).jobs_for("some" => "data")).to eq([])
      end

      it 'returns the assigned step' do
        MyPipeline.send("#{attr}=", :my_step_class)
        expect(MyPipeline.send(attr)).to be(:my_step_class)
      end

      it 'cannot be re-assigned (since there cannot be multiple of the same boundary)' do
        MyPipeline.send("#{attr}=", :A)
        expect {
          MyPipeline.send("#{attr}=", :B)
        }.to raise_error(Pipeline::BoundaryStepAlreadySetError)
      end
    end

    describe ".initial_step" do
      it_behaves_like 'a dependency boundary', :initial_step
    end

    describe '.terminal_step' do
      it_behaves_like 'a dependency boundary', :terminal_step
    end

    describe ".matching_older_unfinished_job_batches", :redis do
      before do
        MyPipeline.configure do |c|
          c.batch_list_key { |hash| hash[:key] || hash["key"] }
        end
      end

      let(:baseline_time)  { Time.utc(2012, 12, 20, 12, 30) }
      let(:main_key)       { "123" }
      let(:main_job_batch) { create_job_batch }

      def create_job_batch(overrides = {})
        batch_data = { key: main_key }.merge(overrides)
        job_batch_list = MyPipeline.job_batch_list_for(batch_data)

        Timecop.freeze(overrides.delete(:time) || baseline_time) do
          return job_batch_list.create_new_batch(batch_data)
        end
      end

      subject { MyPipeline.matching_older_unfinished_job_batches(main_job_batch) }

      it 'includes matching older unfinished job batches' do
        batch = create_job_batch(time: baseline_time - 10)

        expect(batch.created_at).to be < main_job_batch.created_at
        expect(batch).to_not be_complete

        expect(subject).to include(batch)
      end

      it 'does not include older unfinished job batches that do not match' do
        batch = create_job_batch(key: "other", time: baseline_time - 10)

        expect(batch.created_at).to be < main_job_batch.created_at
        expect(batch).to_not be_complete

        expect(subject).not_to include(batch)
      end

      it 'does not include matching newer unfinished job batches' do
        batch = create_job_batch(time: baseline_time + 10)

        expect(batch.created_at).to be > main_job_batch.created_at
        expect(batch).to_not be_complete

        expect(subject).not_to include(batch)
      end

      it 'does not include matching older finished job batches' do
        batch = create_job_batch(time: baseline_time - 10)
        expect(batch.created_at).to be < main_job_batch.created_at

        batch.add_job("some-jid")
        batch.mark_job_as_complete("some-jid")
        expect(batch).to be_complete

        expect(subject).not_to include(batch)
      end
    end
  end
end

