require 'spec_helper'
require 'timecop'
require 'plines/pipeline'
require 'plines/step'
require 'plines/enqueued_job'
require 'plines/job_batch'
require 'plines/configuration'

module Plines
  describe JobBatch, :redis do
    describe ".find" do
      it 'finds a previously created job batch' do
        batch = JobBatch.create(pipeline_module, "a", {})
        batch2 = JobBatch.find(pipeline_module, "a")
        expect(batch2).to eq(batch)
      end

      it 'raises an error if it cannot find an existing one' do
        expect {
          JobBatch.find(pipeline_module, "a")
        }.to raise_error(JobBatch::CannotFindExistingJobBatchError)
      end
    end

    describe ".create" do
      it 'raises an error if a job batch with the given id has already been created' do
        batch = JobBatch.create(pipeline_module, "a", {})
        expect {
          JobBatch.create(pipeline_module, "a", {})
        }.to raise_error(JobBatch::JobBatchAlreadyCreatedError)
      end
    end

    it 'is uniquely identified by the id' do
      j1 = JobBatch.create(pipeline_module, "a", {})
      j2 = JobBatch.create(pipeline_module, "b", {})
      j3 = JobBatch.find(pipeline_module, "a")

      expect(j1).to eq(j3)
      expect(j1).to eql(j3)
      expect(j1).not_to eq(j2)
      expect(j1).not_to eql(j2)

      set = Set.new
      set << j1 << j2 << j3
      expect(set.map(&:object_id)).to match_array [j1.object_id, j2.object_id]
    end

    it 'remembers what pipeline it is for' do
      j1 = JobBatch.create(pipeline_module, "a", {})
      expect(j1.pipeline).to be(pipeline_module)
    end

    let(:t1) { Time.new(2012, 4, 1) }
    let(:t2) { Time.new(2012, 5, 1) }

    it 'remembers when it was created' do
      Timecop.freeze(t1) { JobBatch.create(pipeline_module, "a", {}) }
      j2 = JobBatch.find(pipeline_module, "a")

      expect(j2.created_at).to eq(t1)
    end

    it 'remembers the job batch data' do
      batch = JobBatch.create(pipeline_module, "a", "name" => "Bob", "age" => 13)
      expect(batch.data).to eq("name" => "Bob", "age" => 13)
    end

    describe "#data" do
      it 'returns nil if the job batch was created before we stored the batch data' do
        batch = JobBatch.create(pipeline_module, "a", "name" => "Bob", "age" => 13)
        batch.meta.delete(JobBatch::BATCH_DATA_KEY) # simulate it having been saved w/o this

        expect(batch.data).to be(nil)
      end
    end

    describe "#add_job" do
      it 'adds a job and the external dependencies' do
        batch = JobBatch.create(pipeline_module, "foo", {})
        batch.add_job "abc", "bar", "bazz"
        expect(pipeline_module.redis.smembers("job_batch:foo:pending_job_jids")).to match_array %w[ abc ]
        expect(EnqueuedJob.new("abc").pending_external_dependencies).to match_array ["bar", "bazz"]
      end

      it 'returns the newly added job' do
        batch = JobBatch.create(pipeline_module, "foo", {})
        job = batch.add_job "abc", "bar", "bazz"
        expect(job).to be_an(EnqueuedJob)
        expect(job.jid).to eq("abc")
      end
    end

    describe "#job_jids" do
      it "returns all job jids, even when some have been completed" do
        batch = JobBatch.create(pipeline_module, "foo", {}) do |jb|
          jb.add_job("a"); jb.add_job("b"); jb.add_job("c")
        end

        expect(batch.job_jids.to_a).to match_array %w[ a b c ]
        batch.mark_job_as_complete("a")
        expect(batch.job_jids.to_a).to match_array %w[ a b c ]
      end
    end

    describe "#qless_jobs" do
      it "returns all the qless job instances" do
        batch = JobBatch.create(pipeline_module, "foo", {}) do |jb|
          jb.add_job("a")
        end

        jobs = fire_double("Qless::ClientJobs")
        batch.pipeline.qless.stub(jobs: jobs)
        jobs.stub(:[]).with("a") { :the_job }

        expect(batch.qless_jobs).to eq([:the_job])
      end
    end

    describe "#jobs" do
      it "returns all the enqueued job instances" do
        batch = JobBatch.create(pipeline_module, "foo", {}) do |jb|
          jb.add_job("a")
        end

        job = batch.jobs.first
        expect(job).to be_a(EnqueuedJob)
        expect(job.jid).to eq("a")
      end
    end

    describe "#mark_job_as_complete" do
      let!(:batch) { JobBatch.create(pipeline_module, "foo", {}) }

      before do
        expect(P).to respond_to(:set_expiration_on)
        P.stub(:set_expiration_on)
      end

      it "moves a jid from the pending to the complete set" do
        batch.add_job("a")

        expect(batch.pending_job_jids).to include("a")

        expect(batch.completed_job_jids).not_to include("a")

        batch.mark_job_as_complete("a")

        expect(batch.pending_job_jids).not_to include("a")
        expect(batch.completed_job_jids).to include("a")
      end

      it "raises an error if the given jid is not in the pending set" do
        expect(batch.completed_job_jids).not_to include("a")
        expect { batch.mark_job_as_complete("a") }.to raise_error(JobBatch::JobNotPendingError)
        expect(batch.completed_job_jids).not_to include("a")
      end

      it 'sets the completed_at timestamp when the last job is marked as complete' do
        batch.pending_job_jids << "a" << "b"

        expect(batch.completed_at).to be_nil
        batch.mark_job_as_complete("a")
        expect(batch.completed_at).to be_nil
        Timecop.freeze(t2) { batch.mark_job_as_complete("b") }
        expect(batch.completed_at).to eq(t2)
      end

      it 'expires the redis keys for the batch data' do
        expired_keys = Set.new
        P.stub(:set_expiration_on) do |*args|
          expired_keys.merge(args)
        end

        batch.add_job("a", "foo")
        batch.add_job("b")

        expect(P.redis.keys).not_to be_empty

        batch.resolve_external_dependency("foo")

        batch.mark_job_as_complete("a")
        expect(expired_keys).to be_empty

        batch.mark_job_as_complete("b")
        expect(expired_keys.to_a).to include(*P.redis.keys)
      end
    end

    describe "#complete?" do
      it 'returns false when there are no pending or completed jobs' do
        batch = JobBatch.create(pipeline_module, "foo", {})
        expect(batch).not_to be_complete
      end

      it 'returns false when there are pending jobs and completed jobs' do
        batch = JobBatch.create(pipeline_module, "foo", {})
        batch.pending_job_jids << "a"
        batch.completed_job_jids << "b"
        expect(batch).not_to be_complete
      end

      it 'returns true when there are only completed jobs' do
        batch = JobBatch.create(pipeline_module, "foo", {})
        batch.completed_job_jids << "b"
        expect(batch).to be_complete
      end
    end

    describe "#pending_jobs" do
      it "shows all pending jobs" do
        batch = JobBatch.create(pipeline_module, "foo", {})
        batch.add_job("a")
        expect(batch.pending_qless_jobs).to eq([pipeline_module.qless.jobs["a"]])
      end
    end

    shared_examples_for "updating a job batch external dependency" do |set_name|
      it "updates the dependency resolved on all jobs that have it" do
        batch = JobBatch.create(pipeline_module, "foo", {})
        jida_job = batch.add_job("jida", "foo")
        jidb_job = batch.add_job("jidb", "foo")

        expect(jida_job.pending_external_dependencies).to include("foo")
        expect(jidb_job.pending_external_dependencies).to include("foo")

        update_dependency(batch, "foo")

        expect(jida_job.pending_external_dependencies).not_to include("foo")
        expect(jidb_job.pending_external_dependencies).not_to include("foo")

        expect(jida_job.send(set_name)).to include("foo")
        expect(jidb_job.send(set_name)).to include("foo")
      end

      def queue_for(jid)
        pipeline_module.qless.jobs[jid].queue_name
      end

      step_class(:Klass) do
        qless_options do |q|
          q.queue = "some_queue"
        end
      end

      it "moves the job into it's configured queue when it no longer has pending external dependencies" do
        jid = pipeline_module.awaiting_external_dependency_queue.put(P::Klass, {})
        batch = JobBatch.create(pipeline_module, "foo", {})
        batch.add_job(jid, "foo", "bar")

        update_dependency(batch, "foo")
        expect(queue_for(jid)).to eq(pipeline_module.awaiting_external_dependency_queue.name)
        update_dependency(batch, "bar")
        expect(queue_for(jid)).to eq(P::Klass.processing_queue.name)
      end
    end

    describe "#resolve_external_dependency" do
      it_behaves_like "updating a job batch external dependency", :resolved_external_dependencies do
        def update_dependency(batch, name)
          batch.resolve_external_dependency(name)
        end
      end

      it 'does not attempt to resolve the dependency on jobs that do not have it' do
        batch = JobBatch.create(pipeline_module, "foo", {})
        jida_job = batch.add_job("jida", "foo")
        jidb_job = batch.add_job("jidb")

        EnqueuedJob.stub(:new).with("jida") { jida_job }
        EnqueuedJob.stub(:new).with("jidb") { jidb_job }

        expect(jidb_job).to respond_to(:resolve_external_dependency)
        jidb_job.should_not_receive(:resolve_external_dependency)
        jida_job.should_receive(:resolve_external_dependency)

        batch.resolve_external_dependency("foo")
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
        batch = JobBatch.create(pipeline_module, "foo", {})
        jida_job = batch.add_job("jida", "foo")
        jidb_job = batch.add_job("jidb", "foo")

        batch.timeout_external_dependency("foo", "jida")
        expect(jida_job.timed_out_external_dependencies).to include("foo")
        expect(jidb_job.timed_out_external_dependencies).not_to include("foo")
      end
    end

    describe "#has_unresolved_external_dependency?" do
      let(:batch) { JobBatch.create(pipeline_module, "foo", {}) }

      it 'returns true if the batch has the given external dependency' do
        batch.add_job("jida", "foo")
        expect(batch).to have_unresolved_external_dependency("foo")
      end

      it 'returns false if the batch does not have the given external dependency' do
        expect(batch).not_to have_unresolved_external_dependency("foo")
      end

      it 'does not depend on in-process cached state that is not there for an instance in another process' do
        batch.add_job("jida", "foo")
        other_instance = JobBatch.find(pipeline_module, batch.id)
        expect(other_instance).to have_unresolved_external_dependency("foo")
        expect(other_instance).not_to have_unresolved_external_dependency("bar")
      end

      it 'returns false if the given external dependency has been resolved' do
        batch.add_job("jida", "foo")
        batch.resolve_external_dependency("foo")
        expect(batch).not_to have_unresolved_external_dependency("foo")
      end

      it 'returns true if the given external dependency timed out' do
        batch.add_job("jida", "foo")
        batch.timeout_external_dependency("foo", "jida")
        expect(batch).to have_unresolved_external_dependency("foo")
      end
    end

    describe "#cancel!" do
      step_class(:Foo)
      let(:jid)    { pipeline_module.default_queue.put(P::Foo, {}) }
      let!(:batch) { JobBatch.create(pipeline_module, "foo", {}) { |jb| jb.add_job(jid) } }

      before do
        expect(P).to respond_to(:set_expiration_on)
        P.stub(:set_expiration_on)
      end

      it 'cancels all qless jobs' do
        expect(pipeline_module.default_queue.length).to be > 0
        batch.cancel!
        expect(pipeline_module.default_queue.length).to eq(0)
      end

      it 'keeps track of whether or not cancellation has occurred' do
        expect(batch).not_to be_cancelled
        batch.cancel!
        expect(batch).to be_cancelled
      end

      it 'expires the redis keys for the batch data' do
        expired_keys = Set.new
        P.stub(:set_expiration_on) do |*args|
          expired_keys.merge(args)
        end

        batch.cancel!

        expect(P.redis.keys).not_to be_empty
        expect(expired_keys.to_a).to include(*P.redis.keys.grep(/job_batch/))
      end

      it 'notifies observers that it has been cancelled' do
        notified_batch = nil
        pipeline_module.configuration.after_job_batch_cancellation do |jb|
          notified_batch = jb
        end

        expect { batch.cancel! }.to change { notified_batch }.from(nil).to(batch)
      end
    end
  end
end

