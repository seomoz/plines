require 'timecop'
require 'date'
require 'plines/pipeline'
require 'plines/step'
require 'plines/enqueued_job'
require 'plines/job_batch'
require 'plines/configuration'

module Plines
  RSpec.describe JobBatch, :redis do
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
      after { Timecop.return }

      it 'raises an error if a job batch with the given id has already been created' do
        JobBatch.create(qless, pipeline_module, "a", {})
        expect {
          JobBatch.create(qless, pipeline_module, "a", {})
        }.to raise_error(JobBatch::JobBatchAlreadyCreatedError)
      end

      it 'stores some state indicating the create is in progress' do
        time_1 = Time.utc(2014, 5, 20, 12, 30, 12)
        time_2 = Time.utc(2014, 5, 20, 12, 30, 18)
        Timecop.freeze(time_1)

        value_in_block = nil
        JobBatch.create(qless, pipeline_module, "a", {}) do
          # Use a different instance to demonstrate the state is persistent
          batch = JobBatch.find(qless, pipeline_module, "a")
          Timecop.freeze(time_2)
          value_in_block = batch.creation_in_progress?
        end

        expect(value_in_block).to be true

        batch = JobBatch.find(qless, pipeline_module, "a")
        expect(batch.creation_in_progress?).to be false

        expect(batch.creation_started_at).to eq(time_1)
        expect(batch.created_at).to eq(time_1)
        expect(batch.creation_completed_at).to eq(time_2)
      end

      it 'stores any extra options in the metadata' do
        special_options = {
          timeout_reduction: 10,
          reason: "because", spawned_from_id: '23'
        }

        jb = JobBatch.create(qless, pipeline_module, "a", {}, special_options.merge("foo" => 17))
        expect(jb.create_options).to eq("foo" => 17)
        # check indifferent access
        expect(jb.create_options[:foo]).to eq(17)
        expect(jb.meta.all).not_to include("foo")
      end

      class RedisLogger < BasicObject
        attr_reader :commands

        def initialize(redis)
          @redis    = redis
          @commands = []
        end

        def respond_to_missing?(name, include_private = false)
          @redis.respond_to?(name, include_private) || super
        end

        def method_missing(name, *args, &block)
          @commands << name
          @redis.public_send(name, *args, &block)
        end
      end

      it 'sets the metadata atomically to ensure a partial batch does not get created' do
        allow(qless).to receive_messages(redis: RedisLogger.new(redis))
        JobBatch.create(qless, pipeline_module, "a", { a: 5 }) { }

        # hmset allows atomic setting of multiple keys in bulk.
        # hsetnx does not.
        expect(qless.redis.commands).to include(:hmset)
        expect(qless.redis.commands).not_to include(:hsetnx)
      end
    end

    context "a JobBatch from before we changed the creation meta attributes" do
      it 'still indicates creation is in progress when indicated by the old attributes' do
        jb = JobBatch.send(:new, qless, pipeline_module, "a")
        jb.meta[:created_at] = Time.now.getutc.iso8601
        jb.meta[:creation_in_progress] = 1

        expect(jb.creation_in_progress?).to be true
      end

      it 'does not indicate creation is in progress when indicated by the old attributes' do
        jb = JobBatch.send(:new, qless, pipeline_module, "a")
        jb.meta[:created_at] = Time.now.getutc.iso8601

        expect(jb.creation_in_progress?).to be false
      end

      it 'falls back to `created_at` when `creation_started_at` is not set' do
        jb = JobBatch.send(:new, qless, pipeline_module, "a")
        time = Time.utc(2014, 5, 24, 12, 30, 12)
        jb.meta[:created_at] = time.getutc.iso8601

        expect(jb.creation_started_at).to eq time
        expect(jb.created_at).to eq time
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

    it 'remembers the reason it was created' do
      j1 = JobBatch.create(qless, pipeline_module, "a", {}, reason: "foobar")
      expect(j1.creation_reason).to eq("foobar")
      expect(j1.meta.keys).not_to include("reason")
    end

    it 'stores no creation reason when none is given' do
      j1 = JobBatch.create(qless, pipeline_module, "a", {})
      expect(j1.creation_reason).to eq(nil)
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

        keys = JSON.parse(batch.meta[JobBatch::EXT_DEP_KEYS_KEY])
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
        JobBatch.create(qless, pipeline_module, "foo", {}) do |b|
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

    describe "#awaiting_external_dependency?" do
      let(:batch) do
        JobBatch.create(qless, pipeline_module, "foo", {}) do |b|
          b.add_job('job_1', 'resolved', 'awaiting')
          b.add_job('job_2', 'timed_out')
        end
      end

      before do
        batch.resolve_external_dependency('resolved')
        batch.timeout_external_dependency('timed_out', 'job_2')
      end

      it 'returns true for unresolved external dependencies that have not yet timed out' do
        expect(batch.awaiting_external_dependency?('awaiting')).to be true
      end

      it 'returns false for resolved external dependencies' do
        expect(batch.awaiting_external_dependency?('resolved')).to be false
      end

      it 'returns false for timed out external dependencies' do
        expect(batch.awaiting_external_dependency?('timed_out')).to be false
      end
    end

    describe "#timeout_reduction" do
      it 'persists between redis roundtrips' do
        JobBatch.create(qless, pipeline_module, "a", {},
                        timeout_reduction: 12)
        found = JobBatch.find(qless, pipeline_module, "a")
        expect(found.timeout_reduction).to eq(12)
      end

      it 'defaults to 0' do
        created = JobBatch.create(qless, pipeline_module, "a", {})
        expect(created.timeout_reduction).to eq(0)
      end

      it 'has a value of 0 when there is no record in redis (such as for ' +
         'in-flight batches created before this feature was added)' do
        created = JobBatch.create(qless, pipeline_module, "a", {})
        expect(created.meta.keys).to include("timeout_reduction")
        created.meta.delete("timeout_reduction")

        found = JobBatch.find(qless, pipeline_module, "a")
        expect(found.timeout_reduction).to eq(0)
      end

      it 'does not require a redis call for a newly created job batch' do
        created = JobBatch.create(qless, pipeline_module, "a", {},
                                  timeout_reduction: 3)
        expect(created).not_to receive(:meta) # meta is how it interfaces to redis for this data

        expect(created.timeout_reduction).to eq(3)
      end
    end

    describe "#spawn_copy" do
      let(:batch) { JobBatch.create(qless, pipeline_module, "a",
                                    { "num" => 2, 'b' => 1 }) }

      it 'enqueues a new job batch with the same data as this batch' do
        expect(pipeline_module).to receive(:enqueue_jobs_for)
                       .with({ "num" => 2, 'b' => 1 }, anything)

        batch.spawn_copy
      end

      it 'merges the provided data with the batch data' do
        expect(pipeline_module).to receive(:enqueue_jobs_for)
                       .with({ "num" => 3, 'b' => 1, 'foo' => 4 }, anything)

        batch.spawn_copy do |options|
          options.data_overrides = { 'num' => 3, 'foo' => 4 }
        end
      end

      it 'merges properly when the overrides hash uses symbols' do
        expect(pipeline_module).to receive(:enqueue_jobs_for)
                       .with({ "num" => 3, 'b' => 1, 'foo' => 4 }, anything)

        batch.spawn_copy do |options|
          options.data_overrides = { num: 3, foo: 4 }
        end
      end

      it 'passes along the configured timeout reduction' do
        expect(pipeline_module).to receive(:enqueue_jobs_for)
                       .with(anything, hash_including(timeout_reduction: 23))

        batch.spawn_copy do |options|
          options.timeout_reduction = 23
        end
      end

      it 'passes along a default timeout reduction of 0 when none is set' do
        expect(pipeline_module).to receive(:enqueue_jobs_for)
                       .with(anything, hash_including(timeout_reduction: 0))

        batch.spawn_copy
      end

      it 'passes a long the configured reason' do
        expect(pipeline_module).to receive(:enqueue_jobs_for)
                       .with(anything, hash_including(reason: "foo"))

        batch.spawn_copy do |opt|
          opt.reason = "foo"
        end
      end

      it 'passes its id through as the spawned_from_id' do
        expect(pipeline_module).to receive(:enqueue_jobs_for)
                       .with(anything, hash_including(spawned_from_id: batch.id))

        batch.spawn_copy
      end

      it 'returns the new job batch' do
        allow(pipeline_module).to receive(:enqueue_jobs_for) { :the_new_batch }
        expect(batch.spawn_copy).to eq(:the_new_batch)
      end
    end

    describe "#spawned_from" do
      it 'returns nil by default' do
        batch = JobBatch.create(qless, pipeline_module, "a", {})
        expect(batch.spawned_from).to be_nil
      end

      def create_batches
        b1 = JobBatch.create(qless, pipeline_module, "1", {})
        b2 = JobBatch.create(qless, pipeline_module, "2", {}, spawned_from_id: b1.id)

        return b1, b2
      end

      it 'returns the job batch instance corresponding to the originally ' +
         'provided spawned_from_id' do
        b1, b2 = create_batches

        expect(b2.spawned_from).to eq(b1)
      end

      it 'is memoized' do
        _, b2 = create_batches

        expect(b2.spawned_from).to be(b2.spawned_from)
      end
    end

    describe "#spawned_from_id" do
      it 'returns nil by default' do
        batch = JobBatch.create(qless, pipeline_module, "a", {})
        expect(batch.spawned_from_id).to be_nil
      end

      it 'returns the spawned_from_id value even if the source batch is no longer in redis' do
        batch = JobBatch.create(qless, pipeline_module, "a", {}, spawned_from_id: '23')
        expect(batch.spawned_from_id).to eq('23')
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
        JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
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
        batch.complete_job(qless_job_for "a")
        expect(batch.job_jids.to_a).to match_array %w[ a b c ]
      end
    end

    describe "#qless_jobs" do
      it "returns all the qless job instances" do
        batch = JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
          jb.add_job("a")
        end

        allow(qless).to receive_messages(jobs: { "a" => :the_job })
        expect(batch.qless_jobs).to eq([:the_job])
      end

      it 'ignores nil jobs' do
        batch = JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
          jb.add_job("a")
          jb.add_job("b")
        end

        allow(qless).to receive_messages(jobs: { "a" => nil, "b" => :the_job })
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

    def expired_keys
      redis.keys.select do |key|
        redis.pttl(key) != -1
      end
    end

    describe "#complete_job" do
      def create_batch_with_job
        batch = JobBatch.create(qless, pipeline_module, "foo", {})
        batch.add_job("a")
        job = qless_job_for("a")

        return batch, job
      end

      it 'completes the Qless job' do
        batch, job = create_batch_with_job
        expect(job.state).to eq("running")

        expect {
          batch.complete_job(job)
        }.to change { job.state_changed? }.from(false).to(true)

        expect(qless.jobs[job.jid].state).to eq("complete")
      end

      it 'raises an error if the job cannot be completed' do
        batch, job = create_batch_with_job

        job.complete

        expect {
          expect {
            batch.complete_job(job)
          }.to raise_error(Qless::Job::CantCompleteError)
        }.not_to change { batch.pending_job_jids.to_a }
      end

      it 'triggers job complete callbacks' do
        events = []

        batch, job = create_batch_with_job

        job.before_complete { events << :before_complete }
        job.after_complete { events << :after_complete }

        batch.complete_job(job)

        expect(events).to eq([:before_complete, :after_complete])
      end

      it "moves a jid from the pending to the complete set" do
        batch, job = create_batch_with_job

        expect(batch.pending_job_jids).to include("a")
        expect(batch.completed_job_jids).not_to include("a")

        batch.complete_job(job)

        expect(batch.pending_job_jids).not_to include("a")
        expect(batch.completed_job_jids).to include("a")
      end

      it "does not complete the job if it is not in the pending set" do
        batch, job = create_batch_with_job
        batch.pending_job_jids.delete(job.jid)

        expect(batch.completed_job_jids).not_to include("a")
        expect { batch.complete_job(qless_job_for "a") }.to raise_error(JobBatch::JobNotPendingError)
        expect(batch.completed_job_jids).not_to include("a")
        expect(qless.jobs[job.jid].state).to eq("running")
      end

      it 'sets the completed_at timestamp when the last job is marked as complete' do
        batch = JobBatch.create(qless, pipeline_module, "foo", {})

        batch.pending_job_jids << "a" << "b"

        expect(batch.completed_at).to be_nil
        batch.complete_job(qless_job_for "a")
        expect(batch.completed_at).to be_nil
        Timecop.freeze(t2) { batch.complete_job(qless_job_for "b") }
        expect(batch.completed_at).to eq(t2)
      end

      it 'expires the redis keys for the batch data' do
        batch = JobBatch.create(qless, pipeline_module, "foo", {}) do |jb|
          jb.add_job("a", "foo", "bar")
          jb.add_job("b")
        end

        batch.track_timeout_job("bar", "some_timeout_jid")

        expect(redis.keys).not_to be_empty

        batch.resolve_external_dependency("foo")

        batch.complete_job(qless_job_for "a")
        expect(expired_keys).to be_empty

        batch.complete_job(qless_job_for "b")
        expect(expired_keys.to_a).to include(*redis.keys.grep(/^plines/))
      end
    end

    describe "#complete?" do
      it 'returns true if the job batch has a completed_at timestamp' do
        batch = JobBatch.create(qless, pipeline_module, "foo", {})
        batch.meta['completed_at'] = Time.now.getutc.iso8601
        expect(batch).to be_complete
      end

      it 'returns false if it lacks a completed_at timestamp' do
        batch = JobBatch.create(qless, pipeline_module, "foo", {})
        expect(batch).not_to be_complete
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
        allow(qless).to receive_messages(jobs: { "a" => "job a", "b" => "job b", "c" => "job c" })
      end

      it "returns qless job instances" do
        expect(batch.pending_qless_jobs).to match_array(["job a", "job b", "job c"])
      end

      it "ignores non-pending jids" do
        batch.complete_job(qless_job_for "a")
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
        expect { update_dependency(batch, "bar") }.to move_job(jid).to_queue(P::Klass.processing_queue_for({}))
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

        allow(EnqueuedJob).to receive(:new).with(qless, pipeline_module, "jida") { jida_job }
        allow(EnqueuedJob).to receive(:new).with(qless, pipeline_module, "jidb") { jidb_job }

        expect(jidb_job).to respond_to(:resolve_external_dependency)
        expect(jidb_job).not_to receive(:resolve_external_dependency)
        expect(jida_job).to receive(:resolve_external_dependency)

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
        expect(batch.timeout_job_jid_sets["foo"].size).to eq(1)
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

    describe 'in_terminal_state?' do
      let(:batch) { JobBatch.create(qless, pipeline_module, "foo", {}) }

      it 'returns true for a cancelled job batch' do
        batch.cancel
        expect(batch.in_terminal_state?).to be true
      end

      it 'returns true for a complete job batch' do
        batch.meta['completed_at'] = Time.now.getutc.iso8601
        expect(batch.in_terminal_state?).to be true
      end

      it 'returns false for a job batch that is incomplete and not cancelled' do
        expect(batch.in_terminal_state?).to be false
      end
    end

    context 'for destructively modifying a job batch' do
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

      def complete_batch
        batch.complete_job(qless_job_for jid_1)
        batch.complete_job(qless_job_for jid_2)

        expect(batch).to be_complete
      end

      shared_examples_for "a delete method" do |method|
        define_method :delete do
          batch.public_send(method)
        end

        def plines_keys
          redis.keys('plines:*')
        end

        it 'deletes all plines keys from redis for a completed job batch' do
          complete_batch
          expect(plines_keys).not_to be_empty
          delete
          expect(plines_keys).to be_empty
        end

        it 'deletes all plines keys from redis for a cancelled job batch' do
          batch.cancel
          expect(plines_keys).not_to be_empty
          delete
          expect(plines_keys).to be_empty
        end
      end

      describe '#delete' do
        it_behaves_like 'a delete method', :delete do
          it 'raises an error if the batch is not in a terminal state' do
            expect { batch.delete }.to raise_error(JobBatch::CannotDeleteError)
            expect(batch).not_to be_complete
            expect(batch).not_to be_cancelled
          end
        end
      end

      describe '#delete!' do
        it_behaves_like 'a delete method', :delete! do
          it 'cancels all qless jobs' do
            batch.complete_job(qless_job_for jid_2)
            expect(default_queue.length).to be > 0
            batch.delete!
            expect(default_queue.length).to eq(0)
          end
        end
      end

      describe "a job batch that has only the old cancellation state" do
        it 'still indicates it was cancelled' do
          expect {
            batch.meta["cancelled"] = '1'
          }.to change { batch.cancelled? }.from(false).to(true)
        end
      end

      shared_examples_for "a cancellation method" do
        it 'cancels all qless jobs, including those that it thinks are complete' do
          batch.complete_job(qless_job_for jid_2)
          expect(default_queue.length).to be > 0
          cancel
          expect(default_queue.length).to eq(0)
        end

        context 'if qless silently fails to cancel some jobs' do
          it 'raises an error to indicate the cancellation failure' do
            allow(qless).to receive(:bulk_cancel) # to make it silent no-op
            expect {
              cancel
            }.to raise_error(JobBatch::SomeJobsFailedToCancelError)
          end
        end

        it 'does not consider a complete job to be a failed cancellation' do
          default_queue.pop.complete
          expect { cancel }.not_to raise_error
        end

        it 'keeps track of whether or not cancellation has occurred' do
          expect(batch).not_to be_cancelled
          cancel
          expect(batch).to be_cancelled
        end

        it 'returns a truthy value' do
          expect(cancel).to be true
        end

        it 'expires the redis keys for the batch data' do
          expect(expired_keys).to be_empty
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

        it 'returns true when it succeeds' do
          expect(cancel).to be true
        end

        it 'persists the provided cancellation reason' do
          expect {
            cancel(batch, reason: "because I said so")
          }.to change { batch.cancellation_reason }.from(nil).to("because I said so")
        end

        it 'returns `nil` from `cancellation_reason` when none was given' do
          expect {
            cancel(batch)
          }.not_to change { batch.cancellation_reason }.from(nil)
        end

        it 'is a no-op when it has already been cancelled' do
          notified_count = 0
          pipeline_module.configuration.after_job_batch_cancellation do |jb|
            notified_count += 1
          end

          expect { expect(cancel).to be true }.to change { notified_count }.by(1)
          expect { expect(cancel).to be true }.not_to change { notified_count }
        end

        it 'sets the `cancelled_at` timestamp' do
          expect {
            cancel
          }.to change { batch.cancelled_at }.from(nil).to(a_value_within(1).of(Time.now))
        end

        it 'raises an error if the create is currently in progress' do
          expect {
            JobBatch.create(qless, pipeline_module, "bar", {}) do |jb|
              cancel(jb)
            end
          }.to raise_error(JobBatch::CreationInStillInProgressError)
        end

        it 'allows cancellation of a job batch that appears to have gotten stuck while being created' do
          job_batch_creation_time = Date.iso8601('2013-01-01').to_time
          one_week_into_the_future = job_batch_creation_time + 7 * 24 * 60 * 60

          Timecop.freeze(job_batch_creation_time) do
            JobBatch.create(qless, pipeline_module, "bar", {}) do |jb|
              Timecop.freeze(one_week_into_the_future) do
                expect {
                  cancel(jb)
                }.to change { jb.cancelled? }.from(false).to(true)
              end
            end
          end
        end
      end

      describe "#cancel!" do
        it_behaves_like "a cancellation method" do
          def cancel(jb = batch, *args)
            jb.cancel!(*args)
            true # to satisfy `expect(cancel).to be true`
          end

          it 'raises an error when the job is already complete' do
            complete_batch
            expect { batch.cancel! }.to raise_error(JobBatch::CannotCancelError)
            expect(batch).to be_complete
            expect(batch).not_to be_cancelled
          end
        end
      end

      describe "cancel" do
        it_behaves_like "a cancellation method" do
          def cancel(jb = batch, *args)
            jb.cancel(*args)
          end

          it 'returns false and does not cancel when the jobs is already complete' do
            complete_batch
            expect(batch.cancel).to be false
            expect(batch).to be_complete
            expect(batch).not_to be_cancelled
          end
        end
      end
    end
  end
end
