require 'spec_helper'
require 'plines'

module Plines
  describe "Resolving an external dependency", :redis do
    include_context "integration helpers"

    context 'when doing so fails to move a job to its new queue' do
      def resolve_dependency_with_class_unloaded(batch)
        orig_klass = P::A
        hide_const("P::A") # so resolving it fails

        expect {
          batch.resolve_external_dependency("blah")
        }.to raise_error(NameError, /P::A/)

        stub_const("P::A", orig_klass)
      end

      it 'does not leave things in an inconsistent state' do
        create_pipeline_with_step { has_external_dependencies "blah" }
        batch = enqueue_batch
        jid = batch.pending_job_jids.first

        expect {
          resolve_dependency_with_class_unloaded(batch)
        }.not_to move_job(jid)
        expect(batch).to have_unresolved_external_dependency("blah")

        expect {
          batch.resolve_external_dependency("blah")
        }.to move_job(jid).to_queue(P::A.processing_queue)
        expect(batch).not_to have_unresolved_external_dependency("blah")
      end
    end

    def before_move_dependency_name_to_different_set(name, &block)
      redis_set = Class.new(::Redis::Set) do
        define_method(:move) do |_name, *args|
          block.call if _name == name && key.include?('pending')
          super(_name, *args)
        end
      end

      stub_const("Redis::Set", redis_set)
    end

    let(:alternate_redis) { Redis.new(url: redis.id) }

    def pending_set_using_alternate_redis(batch)
      key = batch.jobs.first.send(:pending_ext_deps).key
      ::Redis::Set.new(key, alternate_redis)
    end

    it 'atomically resolves the dependency, trying again if necessary' do
      create_pipeline_with_step { has_external_dependencies "blah" }
      batch = enqueue_batch
      pending_set = pending_set_using_alternate_redis(batch)

      num_tries = 0
      before_move_dependency_name_to_different_set("blah") do
        num_tries += 1
        if num_tries == 1
          pending_set << "another dep"
        else
          pending_set.delete("another dep")
        end
      end

      expect {
        batch.resolve_external_dependency("blah")
      }.to move_job(batch.pending_job_jids.first).to_queue(P::A.processing_queue)

      expect(batch).not_to have_unresolved_external_dependency("blah")
      expect(num_tries).to eq(3)
    end

    it 'gives up atomically resolving the dependency if the keys keep being changed by another client' do
      create_pipeline_with_step { has_external_dependencies "blah" }
      batch = enqueue_batch
      pending_set = pending_set_using_alternate_redis(batch)

      num_tries = 0
      before_move_dependency_name_to_different_set("blah") do
        pending_set << "dep #{num_tries}"
        num_tries += 1
      end

      expect {
        expect {
          batch.resolve_external_dependency("blah")
        }.to raise_error(EnqueuedJob::CannotUpdateExternalDependencyError)
      }.not_to move_job(batch.pending_job_jids.first)

      expect(num_tries).to be < 10
    end
  end
end
