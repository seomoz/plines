require 'spec_helper'
require 'timecop'
require 'plines/pipeline'
require 'plines/step'
require 'plines/enqueued_job'
require 'plines/job_batch'

module Plines
  describe JobBatch, :redis do
    it 'is uniquely identified by the id' do
      j1 = JobBatch.new(pipeline_module, "a")
      j2 = JobBatch.new(pipeline_module, "b")
      j3 = JobBatch.new(pipeline_module, "a")

      j1.should eq(j3)
      j1.should eql(j3)
      j1.should_not eq(j2)
      j1.should_not eql(j2)

      set = Set.new
      set << j1 << j2 << j3
      set.map(&:object_id).should =~ [j1.object_id, j2.object_id]
    end

    it 'remembers what pipeline it is for' do
      j1 = JobBatch.new(pipeline_module, "a")
      j1.pipeline.should be(pipeline_module)
    end

    let(:t1) { Time.new(2012, 4, 1) }
    let(:t2) { Time.new(2012, 5, 1) }

    it 'remembers when it was created' do
      Timecop.freeze(t1) { JobBatch.new(pipeline_module, "a") }
      j2 = nil
      Timecop.freeze(t2) { j2 = JobBatch.new(pipeline_module, "a") }

      j2.created_at.should eq(t1)
    end

    describe "#add_job" do
      it 'adds a job and the external dependencies' do
        batch = JobBatch.new(pipeline_module, "foo")
        batch.add_job "abc", :bar, :bazz
        pipeline_module.redis.smembers("job_batch:foo:pending_job_jids").should =~ %w[ abc ]
        EnqueuedJob.new("abc").pending_external_dependencies.should =~ [:bar, :bazz]
      end

      it 'returns the newly added job' do
        batch = JobBatch.new(pipeline_module, "foo")
        job = batch.add_job "abc", :bar, :bazz
        job.should be_an(EnqueuedJob)
        job.jid.should eq("abc")
      end
    end

    describe "#job_jids" do
      it "returns all job jids, even when some have been completed" do
        batch = JobBatch.new(pipeline_module, "foo") do |jb|
          jb.add_job("a"); jb.add_job("b"); jb.add_job("c")
        end

        batch.job_jids.to_a.should =~ %w[ a b c ]
        batch.mark_job_as_complete("a")
        batch.job_jids.to_a.should =~ %w[ a b c ]
      end
    end

    describe "#qless_jobs" do
      it "returns all the qless job instances" do
        batch = JobBatch.new(pipeline_module, "foo") do |jb|
          jb.add_job("a")
        end

        jobs = fire_double("Qless::ClientJobs")
        batch.pipeline.qless.stub(jobs: jobs)
        jobs.stub(:[]).with("a") { :the_job }

        batch.qless_jobs.should eq([:the_job])
      end
    end

    describe "#jobs" do
      it "returns all the enqueued job instances" do
        batch = JobBatch.new(pipeline_module, "foo") do |jb|
          jb.add_job("a")
        end

        job = batch.jobs.first
        job.should be_a(EnqueuedJob)
        job.jid.should eq("a")
      end
    end

    describe "#mark_job_as_complete" do
      let!(:batch) { JobBatch.new(pipeline_module, "foo") }

      before do
        P.should respond_to(:set_expiration_on)
        P.stub(:set_expiration_on)
      end

      it "moves a jid from the pending to the complete set" do
        batch.add_job("a")

        batch.pending_job_jids.should include("a")

        batch.completed_job_jids.should_not include("a")

        batch.mark_job_as_complete("a")

        batch.pending_job_jids.should_not include("a")
        batch.completed_job_jids.should include("a")
      end

      it "raises an error if the given jid is not in the pending set" do
        batch.completed_job_jids.should_not include("a")
        expect { batch.mark_job_as_complete("a") }.to raise_error(JobBatch::JobNotPendingError)
        batch.completed_job_jids.should_not include("a")
      end

      it 'sets the completed_at timestamp when the last job is marked as complete' do
        batch.pending_job_jids << "a" << "b"

        batch.completed_at.should be_nil
        batch.mark_job_as_complete("a")
        batch.completed_at.should be_nil
        Timecop.freeze(t2) { batch.mark_job_as_complete("b") }
        batch.completed_at.should eq(t2)
      end

      it 'expires the redis keys for the batch data' do
        expired_keys = Set.new
        P.stub(:set_expiration_on) do |*args|
          expired_keys.merge(args)
        end

        batch.add_job("a", :foo)
        batch.add_job("b")

        P.redis.keys.should_not be_empty

        batch.resolve_external_dependency(:foo)

        batch.mark_job_as_complete("a")
        expired_keys.should be_empty

        batch.mark_job_as_complete("b")
        expired_keys.to_a.should include(*P.redis.keys)
      end
    end

    describe "#complete?" do
      it 'returns false when there are no pending or completed jobs' do
        batch = JobBatch.new(pipeline_module, "foo")
        batch.should_not be_complete
      end

      it 'returns false when there are pending jobs and completed jobs' do
        batch = JobBatch.new(pipeline_module, "foo")
        batch.pending_job_jids << "a"
        batch.completed_job_jids << "b"
        batch.should_not be_complete
      end

      it 'returns true when there are only completed jobs' do
        batch = JobBatch.new(pipeline_module, "foo")
        batch.completed_job_jids << "b"
        batch.should be_complete
      end
    end

    describe "#pending_jobs" do
      it "should show all pending jobs" do
        batch = JobBatch.new(pipeline_module, "foo")
        batch.add_job("a")
        batch.pending_qless_jobs.should eq([pipeline_module.qless.jobs["a"]])
      end
    end

    shared_examples_for "updating a job batch external dependency" do |set_name|
      it "updates the dependency resolved on all jobs that have it" do
        batch = JobBatch.new(pipeline_module, "foo")
        jida_job = batch.add_job("jida", :foo)
        jidb_job = batch.add_job("jidb", :foo)

        jida_job.pending_external_dependencies.should include(:foo)
        jidb_job.pending_external_dependencies.should include(:foo)

        update_dependency(batch, :foo)

        jida_job.pending_external_dependencies.should_not include(:foo)
        jidb_job.pending_external_dependencies.should_not include(:foo)

        jida_job.send(set_name).should include(:foo)
        jidb_job.send(set_name).should include(:foo)
      end

      def queue_for(jid)
        pipeline_module.qless.jobs[jid].queue_name
      end

      it 'moves the job into the default queue when it no longer has pending external dependencies' do
        stub_const('Klass', Class.new)
        jid = pipeline_module.awaiting_external_dependency_queue.put(Klass, {})
        batch = JobBatch.new(pipeline_module, "foo")
        batch.add_job(jid, :foo, :bar)

        update_dependency(batch, :foo)
        queue_for(jid).should eq(pipeline_module.awaiting_external_dependency_queue.name)
        update_dependency(batch, :bar)
        queue_for(jid).should eq(pipeline_module.default_queue.name)
      end
    end

    describe "#resolve_external_dependency" do
      it_behaves_like "updating a job batch external dependency", :resolved_external_dependencies do
        def update_dependency(batch, name)
          batch.resolve_external_dependency(name)
        end
      end

      it 'does not attempt to resolve the dependency on jobs that do not have it' do
        batch = JobBatch.new(pipeline_module, "foo")
        jida_job = batch.add_job("jida", :foo)
        jidb_job = batch.add_job("jidb")

        EnqueuedJob.stub(:new).with("jida") { jida_job }
        EnqueuedJob.stub(:new).with("jidb") { jidb_job }

        jidb_job.should respond_to(:resolve_external_dependency)
        jidb_job.should_not_receive(:resolve_external_dependency)
        jida_job.should_receive(:resolve_external_dependency)

        batch.resolve_external_dependency(:foo)
      end
    end

    describe "#timeout_external_dependency" do
      it_behaves_like "updating a job batch external dependency", :timed_out_external_dependencies do
        def update_dependency(batch, name)
          jids = batch.job_jids
          batch.timeout_external_dependency(name, jids)
        end
      end

      it 'only times out the dependency on the given jobs' do
        batch = JobBatch.new(pipeline_module, "foo")
        jida_job = batch.add_job("jida", :foo)
        jidb_job = batch.add_job("jidb", :foo)

        batch.timeout_external_dependency(:foo, "jida")
        jida_job.timed_out_external_dependencies.should include(:foo)
        jidb_job.timed_out_external_dependencies.should_not include(:foo)
      end
    end

    describe "#cancel!" do
      step_class(:Foo)
      let(:jid)    { pipeline_module.default_queue.put(P::Foo, {}) }
      let!(:batch) { JobBatch.new(pipeline_module, "foo") { |jb| jb.add_job(jid) } }

      before do
        P.should respond_to(:set_expiration_on)
        P.stub(:set_expiration_on)
      end

      it 'cancels all qless jobs' do
        pipeline_module.default_queue.length.should be > 0
        batch.cancel!
        pipeline_module.default_queue.length.should eq(0)
      end

      it 'keeps track of whether or not cancellation has occurred' do
        batch.should_not be_cancelled
        batch.cancel!
        batch.should be_cancelled
      end

      it 'expires the redis keys for the batch data' do
        expired_keys = Set.new
        P.stub(:set_expiration_on) do |*args|
          expired_keys.merge(args)
        end

        batch.cancel!

        P.redis.keys.should_not be_empty
        expired_keys.to_a.should include(*P.redis.keys.grep(/job_batch/))
      end
    end
  end
end

