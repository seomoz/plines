require 'spec_helper'
require 'plines/enqueued_job'
require 'plines/pipeline'
require 'plines/configuration'

module Plines
  describe EnqueuedJob, :redis do
    it 'is uniquely identified by the jid' do
      j1 = EnqueuedJob.new(qless, pipeline_module, "a")
      j2 = EnqueuedJob.new(qless, pipeline_module, "b")
      j3 = EnqueuedJob.new(qless, pipeline_module, "a")

      expect(j1).to eq(j3)
      expect(j1).to eql(j3)
      expect(j1).not_to eq(j2)
      expect(j1).not_to eql(j2)

      set = Set.new
      set << j1 << j2 << j3
      expect(set.map(&:object_id)).to match_array [j1.object_id, j2.object_id]
    end

    it 'provides a jid accessor' do
      j1 = EnqueuedJob.new(qless, pipeline_module, "abc")
      expect(j1.jid).to eq("abc")
    end

    describe "#create" do
      it "creates an EnqueuedJob with the given external dependencies" do
        EnqueuedJob.create(qless, pipeline_module, "abc", "foo", "bar")
        ej = EnqueuedJob.new(qless, pipeline_module, "abc")
        expect(ej.pending_external_dependencies).to match_array %w[ foo bar ]
      end
    end

    describe "#qless_job" do
      it 'returns the corresponding qless job' do
        stub_const("P::A", Class.new)
        jid = qless.queues["foo"].put(P::A, {})
        qless_job = EnqueuedJob.new(qless, P, jid).qless_job
        expect(qless_job).to be_a(Qless::Job)
        expect(qless_job.klass).to be(P::A)
      end

      it 'returns nil if no qless job can be found' do
        expect(EnqueuedJob.new(qless, P, "some_jid").qless_job).to be(nil)
      end
    end

    describe "#all_external_dependencies" do
      it "returns pending, resolved and timed out external dependencies" do
        job = EnqueuedJob.create(qless, pipeline_module, "abc", "foo", "bar", "bazz")
        job.resolve_external_dependency("foo")
        job.timeout_external_dependency("bazz")

        expect(job.pending_external_dependencies).not_to be_empty
        expect(job.resolved_external_dependencies).not_to be_empty
        expect(job.timed_out_external_dependencies).not_to be_empty

        expect(job.all_external_dependencies).to match_array %w[ foo bar bazz ]
      end
    end

    describe "#unresolved_external_dependencies" do
      it "returns pending and timed out external dependencies but not resolved ones" do
        job = EnqueuedJob.create(qless, pipeline_module, "abc", "foo", "bar", "bazz")
        job.resolve_external_dependency("foo")
        job.timeout_external_dependency("bazz")

        expect(job.pending_external_dependencies).not_to be_empty
        expect(job.resolved_external_dependencies).not_to be_empty
        expect(job.timed_out_external_dependencies).not_to be_empty

        expect(job.unresolved_external_dependencies).to match_array %w[ bar bazz ]
      end
    end

    describe "#declared_redis_object_keys" do
      it 'returns the keys for each owned object' do
        job = EnqueuedJob.create(qless, pipeline_module, "abc", "foo", "bar", "bazz")
        job.resolve_external_dependency("foo")
        job.timeout_external_dependency("bar")

        keys = job.declared_redis_object_keys
        expect(keys.entries.size).to eq(3)
        expect(keys.grep(/pending/).size).to eq(1)
        expect(keys.grep(/resolved/).size).to eq(1)
        expect(keys.grep(/timed_out/).size).to eq(1)

        expect(job.redis.keys).to include(*keys)
      end
    end

    def put_qless_job
      stub_const("P::A", Class.new)
      allow(P::A).to receive_messages(processing_queue: "processing")
      qless.queues[Pipeline::AWAITING_EXTERNAL_DEPENDENCY_QUEUE].put(P::A, {})
    end

    shared_examples_for "updating an enqueued job external dependency" do |meth, final_set|
      describe "##{meth}" do
        let(:jid) { "abc" }

        it "moves the dependency to the #{final_set} set" do
          EnqueuedJob.create(qless, pipeline_module, jid, "foo", "bar")
          ej = EnqueuedJob.new(qless, pipeline_module, jid)
          ej.send(meth, "bar")
          expect(ej.pending_external_dependencies).to eq(["foo"])
          expect(ej.send(final_set)).to eq(["bar"])
        end

        def jobs_queue(jid)
          qless.jobs[jid].queue.name
        end

        it 'moves the job to its proper queue when all dependencies are resolved' do
          jid = put_qless_job

          EnqueuedJob.create(qless, pipeline_module, jid, "foo", "bar")
          ej = EnqueuedJob.new(qless, pipeline_module, jid)

          expect { ej.send(meth, "bar") }.not_to move_job(jid)
          expect { ej.send(meth, "foo") }.to move_job(jid).to_queue("processing")
        end

        it 'raises an error and does not yield if the given dependency does not exist' do
          yielded = false
          EnqueuedJob.create(qless, pipeline_module, jid)
          ej = EnqueuedJob.new(qless, pipeline_module, jid)
          expect { ej.send(meth, "bazz") { yielded = true } }.to raise_error(ArgumentError)
          expect(yielded).to be false
        end
      end
    end

    it_behaves_like "updating an enqueued job external dependency",
      :timeout_external_dependency, :timed_out_external_dependencies

    it_behaves_like "updating an enqueued job external dependency",
      :resolve_external_dependency, :resolved_external_dependencies

    context 'when resolving a previously timed out dependency' do
      let(:jid) { put_qless_job }
      let(:ej) { EnqueuedJob.create(qless, pipeline_module, jid, "foo", "bar") }

      before { ej.timeout_external_dependency("foo") }

      it 'moves it to the resolved_external_dependencies set' do
        ej.resolve_external_dependency("foo")
        expect(ej.resolved_external_dependencies).to include("foo")
        expect(ej.timed_out_external_dependencies).not_to include("foo")
      end

      it 'does not move the job since it does not affect the number of pending dependencies' do
        expect { ej.resolve_external_dependency("foo") }.not_to move_job(jid)
        ej.timeout_external_dependency("bar")
        expect { ej.resolve_external_dependency("bar") }.not_to move_job(jid)
      end
    end

    it 'cannot timeout a resolved dependency' do
      jid = put_qless_job

      ej = EnqueuedJob.create(qless, pipeline_module, jid, "foo")
      ej.resolve_external_dependency("foo")

      expect { ej.timeout_external_dependency("foo") }.not_to move_job(jid)

      expect(ej.resolved_external_dependencies).to include("foo")
      expect(ej.timed_out_external_dependencies).not_to include("foo")
    end
  end
end

