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
        batch = JobBatch.create(qless, pipeline_module, "a", {})
        batch2 = JobBatch.find(qless, pipeline_module, "a")
        expect(batch2).to eq(batch)
      end

      it 'raises an error if it cannot find an existing one' do
        expect {
          JobBatch.find(qless, pipeline_module, "a")
        }.to raise_error(JobBatch::CannotFindExistingJobBatchError)
      end
    end

    describe ".create" do
      it 'raises an error if a job batch with the given id has already been created' do
        batch = JobBatch.create(qless, pipeline_module, "a", {})
        expect {
          JobBatch.create(qless, pipeline_module, "a", {})
        }.to raise_error(JobBatch::JobBatchAlreadyCreatedError)
      end
    end

    it 'is uniquely identified by the id' do
      j1 = JobBatch.create(qless, pipeline_module, "a", {})
      j2 = JobBatch.create(qless, pipeline_module, "b", {})
      j3 = JobBatch.find(qless, pipeline_module, "a")

      expect(j1).to eq(j3)
      expect(j1).to eql(j3)
      expect(j1).not_to eq(j2)
      expect(j1).not_to eql(j2)

      set = Set.new
      set << j1 << j2 << j3
      expect(set.map(&:object_id)).to match_array [j1.object_id, j2.object_id]
    end

    it 'remembers what pipeline it is for' do
      j1 = JobBatch.create(qless, pipeline_module, "a", {})
      expect(j1.pipeline).to be(pipeline_module)
    end

    let(:t1) { Time.new(2012, 4, 1) }
    let(:t2) { Time.new(2012, 5, 1) }

    it 'remembers when it was created' do
      Timecop.freeze(t1) { JobBatch.create(qless, pipeline_module, "a", {}) }
      j2 = JobBatch.find(qless, pipeline_module, "a")

      expect(j2.created_at).to eq(t1)
    end

    it 'remembers the job batch data' do
      batch = JobBatch.create(qless, pipeline_module, "a", "name" => "Bob", "age" => 13)
      expect(batch.data).to eq("name" => "Bob", "age" => 13)
    end

    it 'exposes the data as an indifferent hash' do
      batch = JobBatch.create(qless, pipeline_module, "a", "name" => "Bob", "age" => 13)
      expect(batch.data[:name]).to eq("Bob")
    end

    describe "#data" do
      it 'returns nil if the job batch was created before we stored the batch data' do
        batch = JobBatch.create(qless, pipeline_module, "a", "name" => "Bob", "age" => 13)
        batch.meta.delete(JobBatch::BATCH_DATA_KEY) # simulate it having been saved w/o this

        expect(batch.data).to be(nil)
      end
    end

    describe '#populate_external_deps_meta' do
      it 'correctly de-dupes existing and new external dependencies' do
        batch = JobBatch.create(qless, pipeline_module, "foo", {})
        batch.meta[JobBatch::EXT_DEP_KEYS_KEY] = JSON.dump(%w[ bar bazz ])
        batch.newly_added_external_deps << 'bar' << 'bazz' << 'bat' << 'foo'
        batch.populate_external_deps_meta {}

        keys = JSON.load(batch.meta[JobBatch::EXT_DEP_KEYS_KEY])
        expect(keys).to match_array(%w[ foo bar bat bazz ])
      end

      context 'when an error is raised while it yields' do
        it 'does not continue to allow jobs to be added with external dependencies' do
          batch = JobBatch.create(qless, pipeline_module, "foo", {})

          expect {
            batch.populate_external_deps_meta { raise "boom" }
          }.to raise_error("boom")

          expect {
            batch.add_job "abc", "bar"
          }.to raise_error(JobBatch::AddingExternalDependencyNotAllowedError)
        end
      end
    end

    describe '#external_deps' do
      it 'returns an array of external dependencies if there are any' do
        batch = JobBatch.create(qless, pipeline_module, "foo", {}) do |b|
          b.add_job('1234', 'foo', 'bar')
          b.add_job('2345', 'bar', 'foo', 'bazz')
        end

        expect(batch.external_deps).to match_array(%w[ foo bar bazz ])
      end

      it 'returns an empty array where there are no external dependencies' do
        batch = JobBatch.create(qless, pipeline_module, "foo", {})
        expect(batch.external_deps).to eq([])
      end
    end

    describe "#timed_out_external_dependencies" do
      let(:batch) do
        batch = JobBatch.create(qless, pipeline_module, "foo", {}) do |b|
          b.add_job('1234', 'foo', 'bar')
          b.add_job('2345', 'bazz')
        end
      end

      it 'returns a blank array by default' do
        expect(batch.timed_out_external_dependencies).to eq([])
      end

      it 'includes any dependencies that have timed out' do
        batch.timeout_external_dependency('bar', '1234')
        expect(batch.timed_out_external_dependencies).to eq(['bar'])

        batch.timeout_external_dependency('bar', '1234') # should not add another entry
        batch.timeout_external_dependency('foo', '1234')
        expect(batch.timed_out_external_dependencies).to match_array(['bar', 'foo'])
      end
    end

    describe "#spawn_copy" do
      let(:batch) { JobBatch.create(qless, pipeline_module, "a",
                                    { "num" => 2, 'b' => 1 }) }

      it 'enqueues a new job batch with the same data as this batch' do
        pipeline_module.should_receive(:enqueue_jobs_for)
                       .with({ "num" => 2, 'b' => 1 }, anything)

        batch.spawn_copy
      end

      it 'merges the provided data with the batch data' do
        pipeline_module.should_receive(:enqueue_jobs_for)
                       .with({ "num" => 3, 'b' => 1, 'foo' => 4 }, anything)

        batch.spawn_copy do |options|
          options.data_overrides = { 'num' => 3, 'foo' => 4 }
        end
      end

      it 'merges properly when the overrides hash uses symbols' do
        pipeline_module.should_receive(:enqueue_jobs_for)
                       .with({ "num" => 3, 'b' => 1, 'foo' => 4 }, anything)

        batch.spawn_copy do |options|
          options.data_overrides = { num: 3, foo: 4 }
        end
      end

      it 'passes along the configured timeout reduction' do
        pipeline_module.should_receive(:enqueue_jobs_for)
                       .with(anything, 23)

        batch.spawn_copy do |options|
          options.timeout_reduction = 23
        end
      end

      it 'passes along a default timeout reduction of 0 when none is set' do
        pipeline_module.should_receive(:enqueue_jobs_for)
                       .with(anything, 0)

        batch.spawn_copy
      end

      it 'returns the new job batch' do
        pipeline_module.stub(:enqueue_jobs_for) { :the_new_batch }
        expect(batch.spawn_copy).to eq(:the_new_batch)
      end
    end

    describe "#add_job" do
      it 'adds a job and the external dependencies' do
        batch = JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
          jb.add_job "abc", "bar", "bazz"
        end
        expect(redis.smembers("plines:P:JobBatch:foo:pending_job_jids")).to match_array %w[ abc ]
        expect(batch.newly_added_external_deps).to match_array(%w[ bar bazz ])
        expect(EnqueuedJob.new(qless, pipeline_module, "abc").pending_external_dependencies).to match_array ["bar", "bazz"]
      end

      it 'returns the newly added job' do
        job = nil
        batch = JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
          job = jb.add_job "abc", "bar", "bazz"
        end
        expect(job).to be_an(EnqueuedJob)
        expect(job.jid).to eq("abc")
      end

      it 'raises an error when attempting to add a job with external deps after the batch was created' do
        batch = JobBatch.create(qless, pipeline_module, "foo", {})
        expect {
          batch.add_job "abc", "bar"
        }.to raise_error(JobBatch::AddingExternalDependencyNotAllowedError)
      end

      it 'allows you to add a job without external deps after the batch is created' do
        batch = JobBatch.create(qless, pipeline_module, "foo", {})
        job = batch.add_job "abc"
        expect(job).to be_an(EnqueuedJob)
        expect(job.jid).to eq("abc")
      end
    end

    describe "#job_jids" do
      it "returns all job jids, even when some have been completed" do
        batch = JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
          jb.add_job("a"); jb.add_job("b"); jb.add_job("c")
        end

        expect(batch.job_jids.to_a).to match_array %w[ a b c ]
        batch.mark_job_as_complete("a")
        expect(batch.job_jids.to_a).to match_array %w[ a b c ]
      end
    end

    describe "#qless_jobs" do
      it "returns all the qless job instances" do
        batch = JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
          jb.add_job("a")
        end

        qless.stub(jobs: { "a" => :the_job })
        expect(batch.qless_jobs).to eq([:the_job])
      end

      it 'ignores nil jobs' do
        batch = JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
          jb.add_job("a")
          jb.add_job("b")
        end

        qless.stub(jobs: { "a" => nil, "b" => :the_job })
        expect(batch.qless_jobs).to eq([:the_job])
      end
    end

    describe "#jobs" do
      it "returns all the enqueued job instances" do
        batch = JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
          jb.add_job("a")
        end

        job = batch.jobs.first
        expect(job).to be_a(EnqueuedJob)
        expect(job.jid).to eq("a")
      end
    end

    describe "#mark_job_as_complete" do
      before do
        expect(redis).to respond_to(:pexpire)
        redis.stub(:pexpire)
      end

      it "moves a jid from the pending to the complete set" do
        batch = JobBatch.create(qless, pipeline_module, "foo", {})

        batch.add_job("a")

        expect(batch.pending_job_jids).to include("a")

        expect(batch.completed_job_jids).not_to include("a")

        batch.mark_job_as_complete("a")

        expect(batch.pending_job_jids).not_to include("a")
        expect(batch.completed_job_jids).to include("a")
      end

      it "raises an error if the given jid is not in the pending set" do
        batch = JobBatch.create(qless, pipeline_module, "foo", {})

        expect(batch.completed_job_jids).not_to include("a")
        expect { batch.mark_job_as_complete("a") }.to raise_error(JobBatch::JobNotPendingError)
        expect(batch.completed_job_jids).not_to include("a")
      end

      it 'sets the completed_at timestamp when the last job is marked as complete' do
        batch = JobBatch.create(qless, pipeline_module, "foo", {})

        batch.pending_job_jids << "a" << "b"

        expect(batch.completed_at).to be_nil
        batch.mark_job_as_complete("a")
        expect(batch.completed_at).to be_nil
        Timecop.freeze(t2) { batch.mark_job_as_complete("b") }
        expect(batch.completed_at).to eq(t2)
      end

      it 'expires the redis keys for the batch data' do
        expired_keys = Set.new
        redis.stub(:pexpire) do |key, time|
          expired_keys << key
        end

        batch = JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
          jb.add_job("a", "foo", "bar")
          jb.add_job("b")
        end

        batch.track_timeout_job("bar", "some_timeout_jid")

        expect(redis.keys).not_to be_empty

        batch.resolve_external_dependency("foo")

        batch.mark_job_as_complete("a")
        expect(expired_keys).to be_empty

        batch.mark_job_as_complete("b")
        expect(expired_keys.to_a).to include(*redis.keys)
      end
    end

    describe "#complete?" do
      it 'returns false when there are no pending or completed jobs' do
        batch = JobBatch.create(qless, pipeline_module, "foo", {})
        expect(batch).not_to be_complete
      end

      it 'returns false when there are pending jobs and completed jobs' do
        batch = JobBatch.create(qless, pipeline_module, "foo", {})
        batch.pending_job_jids << "a"
        batch.completed_job_jids << "b"
        expect(batch).not_to be_complete
      end

      it 'returns true when there are only completed jobs' do
        batch = JobBatch.create(qless, pipeline_module, "foo", {})
        batch.completed_job_jids << "b"
        expect(batch).to be_complete
      end
    end

    describe "#pending_qless_jobs" do
      let(:batch) do
        JobBatch.create(qless, pipeline_module, "foo", {}) do |b|
          b.add_job("a")
          b.add_job("b")
          b.add_job("c")
        end
      end

      before do
        qless.stub(jobs: { "a" => "job a", "b" => "job b", "c" => "job c" })
      end

      it "returns qless job instances" do
        expect(batch.pending_qless_jobs).to match_array(["job a", "job b", "job c"])
      end

      it "ignores non-pending jids" do
        batch.mark_job_as_complete("a")
        expect(batch.pending_qless_jobs).to match_array(["job b", "job c"])
      end

      it 'ignores nil jobs' do
        qless.jobs.delete("a")
        qless.jobs.delete("c")
        expect(batch.pending_qless_jobs).to match_array(["job b"])
      end
    end

    shared_examples_for "updating a job batch external dependency" do |set_name|
      it "updates the dependency resolved on all jobs that have it" do
        jida_job, jidb_job = nil
        batch = JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
          jida_job = jb.add_job("jida", "foo")
          jidb_job = jb.add_job("jidb", "foo")
        end

        expect(jida_job.pending_external_dependencies).to include("foo")
        expect(jidb_job.pending_external_dependencies).to include("foo")

        update_dependency(batch, "foo")

        expect(jida_job.pending_external_dependencies).not_to include("foo")
        expect(jidb_job.pending_external_dependencies).not_to include("foo")

        expect(jida_job.send(set_name)).to include("foo")
        expect(jidb_job.send(set_name)).to include("foo")
      end

      def queue_for(jid)
        qless.jobs[jid].queue_name
      end

      step_class(:Klass) do
        qless_options do |q|
          q.queue = "some_queue"
        end
      end

      it "moves the job into it's configured queue when it no longer has pending external dependencies" do
        jid = qless.queues[Pipeline::AWAITING_EXTERNAL_DEPENDENCY_QUEUE].put(P::Klass, {})
        batch = JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
          jb.add_job(jid, "foo", "bar")
        end

        expect { update_dependency(batch, "foo") }.not_to move_job(jid)
        expect { update_dependency(batch, "bar") }.to move_job(jid).to_queue(P::Klass.processing_queue)
      end
    end

    def enqueue_timeout_job
      qless.queues["timeouts"].put(Qless::Job, {})
    end

    describe "#resolve_external_dependency" do
      it_behaves_like "updating a job batch external dependency", :resolved_external_dependencies do
        def update_dependency(batch, name)
          batch.resolve_external_dependency(name)
        end
      end

      it 'does not attempt to resolve the dependency on jobs that do not have it' do
        jida_job, jidb_job = nil
        batch = JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
          jida_job = jb.add_job("jida", "foo")
          jidb_job = jb.add_job("jidb")
        end

        EnqueuedJob.stub(:new).with(qless, pipeline_module, "jida") { jida_job }
        EnqueuedJob.stub(:new).with(qless, pipeline_module, "jidb") { jidb_job }

        expect(jidb_job).to respond_to(:resolve_external_dependency)
        jidb_job.should_not_receive(:resolve_external_dependency)
        jida_job.should_receive(:resolve_external_dependency)

        batch.resolve_external_dependency("foo")
      end

      it 'cancels the timeout jobs for the given dependencies' do
        batch = JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
          jb.add_job("jida", "foo", "bar")
        end

        timeout_jid_1 = enqueue_timeout_job
        timeout_jid_2 = enqueue_timeout_job
        timeout_jid_3 = enqueue_timeout_job

        batch.track_timeout_job("foo", timeout_jid_1)
        batch.track_timeout_job("foo", timeout_jid_2)
        batch.track_timeout_job("bar", timeout_jid_3)

        batch.resolve_external_dependency("foo")

        expect(qless.jobs[timeout_jid_1]).to be_nil
        expect(qless.jobs[timeout_jid_2]).to be_nil
        expect(qless.jobs[timeout_jid_3]).to be_a(Qless::Job)
      end

      it 'gracefully handle timeout jobs that have already been cancelled' do
        batch = JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
          jb.add_job("jida", "foo")
        end

        timeout_jid = enqueue_timeout_job
        batch.track_timeout_job("foo", timeout_jid)

        qless.jobs[timeout_jid].cancel

        expect {
          batch.resolve_external_dependency("foo")
        }.to change { batch.timeout_job_jid_sets["foo"].to_a }.to([])
      end

      it 'clears the timeout job jid set as it is no longer needed' do
        batch = JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
          jb.add_job("jida", "foo")
        end

        timeout_jid = enqueue_timeout_job
        batch.track_timeout_job("foo", timeout_jid)

        expect {
          batch.resolve_external_dependency("foo")
        }.to change { batch.timeout_job_jid_sets["foo"].to_a }.to([])
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
        jida_job, jidb_job = nil
        batch = JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
          jida_job = jb.add_job("jida", "foo")
          jidb_job = jb.add_job("jidb", "foo")
        end

        batch.timeout_external_dependency("foo", "jida")
        expect(jida_job.timed_out_external_dependencies).to include("foo")
        expect(jidb_job.timed_out_external_dependencies).not_to include("foo")
      end

      it 'does not cancel or delete the timeout job jids' do
        batch = JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
          jb.add_job("jida", "foo")
        end

        timeout_jid = enqueue_timeout_job
        batch.track_timeout_job("foo", timeout_jid)

        batch.timeout_external_dependency("foo", "jida")
        expect(qless.jobs[timeout_jid]).to be_a(Qless::Job)
        expect(batch.timeout_job_jid_sets["foo"]).to have(1).jid
      end
    end

    describe "#has_unresolved_external_dependency?" do
      let(:batch) do
        JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
          jb.add_job("jida", "foo")
        end
      end

      it 'returns true if the batch has the given external dependency' do
        expect(batch).to have_unresolved_external_dependency("foo")
      end

      it 'returns false if the batch does not have the given external dependency' do
        expect(batch).not_to have_unresolved_external_dependency("bar")
      end

      it 'does not depend on in-process cached state that is not there for an instance in another process' do
        other_instance = JobBatch.find(qless, pipeline_module, batch.id)
        expect(other_instance).to have_unresolved_external_dependency("foo")
        expect(other_instance).not_to have_unresolved_external_dependency("bar")
      end

      it 'returns false if the given external dependency has been resolved' do
        batch.resolve_external_dependency("foo")
        expect(batch).not_to have_unresolved_external_dependency("foo")
      end

      it 'returns true if the given external dependency timed out' do
        batch.timeout_external_dependency("foo", "jida")
        expect(batch).to have_unresolved_external_dependency("foo")
      end
    end

    shared_examples_for "a cancellation method" do |method|
      step_class(:Foo)
      let(:default_queue) { qless.queues[Pipeline::DEFAULT_QUEUE] }
      let(:jid_1)  { default_queue.put(P::Foo, {}) }
      let(:jid_2)  { default_queue.put(P::Foo, {}) }
      let!(:batch) do
        JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
          jb.add_job(jid_1)
          jb.add_job(jid_2)
        end
      end

      before do
        expect(redis).to respond_to(:pexpire)
        redis.stub(:pexpire)
      end

      define_method :cancel do
        batch.public_send(method)
      end

      it 'cancels all qless jobs, including those that it thinks are complete' do
        batch.mark_job_as_complete(jid_2)
        expect(default_queue.length).to be > 0
        cancel
        expect(default_queue.length).to eq(0)
      end

      it 'keeps track of whether or not cancellation has occurred' do
        expect(batch).not_to be_cancelled
        cancel
        expect(batch).to be_cancelled
      end

      it 'returns a truthy value' do
        expect(cancel).to be_true
      end

      it 'expires the redis keys for the batch data' do
        expired_keys = Set.new
        redis.stub(:pexpire) do |key, time|
          expired_keys << key
        end

        cancel

        expect(redis.keys).not_to be_empty
        expect(expired_keys.to_a).to include(*redis.keys.grep(/JobBatch/))
      end

      it 'notifies observers that it has been cancelled' do
        notified_batch = nil
        pipeline_module.configuration.after_job_batch_cancellation do |jb|
          notified_batch = jb
        end

        expect { cancel }.to change { notified_batch }.from(nil).to(batch)
      end

      def complete_batch
        batch.mark_job_as_complete(jid_1)
        batch.mark_job_as_complete(jid_2)

        expect(batch).to be_complete
      end

      it 'returns true when it succeeds' do
        expect(cancel).to be_true
      end

      it 'is a no-op when it has already been cancelled' do
        notified_count = 0
        pipeline_module.configuration.after_job_batch_cancellation do |jb|
          notified_count += 1
        end

        expect { expect(cancel).to be_true }.to change { notified_count }.by(1)
        expect { expect(cancel).to be_true }.not_to change { notified_count }
      end
    end

    describe "#cancel!" do
      it_behaves_like "a cancellation method", :cancel! do
        it 'raises an error when the job is already complete' do
          complete_batch
          expect { batch.cancel! }.to raise_error(JobBatch::CannotCancelError)
          expect(batch).to be_complete
          expect(batch).not_to be_cancelled
        end
      end
    end

    describe "cancel" do
      it_behaves_like "a cancellation method", :cancel do
        it 'returns false and does not cancel when the jobs is already complete' do
          complete_batch
          expect(batch.cancel).to be_false
          expect(batch).to be_complete
          expect(batch).not_to be_cancelled
        end
      end
    end
  end
end

