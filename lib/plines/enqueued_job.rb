require 'plines/redis_objects'

module Plines
  # Once a Plines::Job has been enqueued as a Qless job into redis,
  # an EnqueuedJob is used to represent and hold the additional state
  # that Plines needs to track about the job.
  class EnqueuedJob < Struct.new(:pipeline, :jid)
    include Plines::RedisObjectsHelpers

    attr_reader :redis

    def initialize(pipeline, jid, &block)
      @redis = pipeline.redis
      super(pipeline, jid)
      instance_eval(&block) if block
    end

    def self.create(pipeline, jid, *external_dependencies)
      new(pipeline, jid) do
        external_dependencies.each do |dep|
          pending_ext_deps << dep
        end
      end
    end

    def qless_job
      pipeline.qless.jobs[jid]
    end

    def pending_external_dependencies
      pending_ext_deps.members
    end

    def resolved_external_dependencies
      resolved_ext_deps.members
    end

    def timed_out_external_dependencies
      timed_out_ext_deps.members
    end

    def all_external_dependencies
      pending_ext_deps.union(
        resolved_ext_deps, timed_out_ext_deps
      )
    end

    def unresolved_external_dependencies
      pending_ext_deps.union(timed_out_ext_deps)
    end

    def resolve_external_dependency(name)
      update_external_dependency \
        name, resolved_ext_deps, pending_ext_deps, timed_out_ext_deps
    end

    def timeout_external_dependency(name)
      update_external_dependency \
        name, timed_out_ext_deps, pending_ext_deps
    end

  private

    alias id jid # id is needed by Redis::Objects
    set :pending_ext_deps
    set :resolved_ext_deps
    set :timed_out_ext_deps

    CannotUpdateExternalDependencyError = Class.new(StandardError)

    def update_external_dependency(name, destination_set, *source_sets)
      assert_has_external_dependency!(name)

      result = nil

      5.times do
        result = try_to_update(qless_job, name, destination_set, *source_sets)
        break unless result == :needs_retry
      end

      if result == :needs_retry
        raise CannotUpdateExternalDependencyError,
          "Failed to update dependency #{name} after many tries"
      end
    end

    def try_to_update(job, name, destination_set, *source_sets)
      redis.watch(pending_ext_deps.key) do
        pending_deps = self.pending_external_dependencies

        response = redis.multi do
          source_sets.each do |source_set|
            source_set.move(name, destination_set)
          end

          if job && pending_deps == [name]
            job.move(job.klass.processing_queue.name)
          end
        end

        return :needs_retry unless response
      end
    end

    def assert_has_external_dependency!(name)
      results = redis.multi do
        pending_ext_deps.include?(name)
        resolved_ext_deps.include?(name)
        timed_out_ext_deps.include?(name)
      end

      if results.none?
        raise ArgumentError, "EnqueuedJob #{jid} does not have pending " +
                             "external dependency #{name.inspect}"
      end
    end
  end
end

