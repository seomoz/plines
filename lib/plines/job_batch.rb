require 'time'
require 'plines/extensions/redis_objects'

module Plines
  # Represents a group of jobs that are enqueued together as a batch,
  # based on the step dependency graph.
  class JobBatch < Struct.new(:pipeline, :id)
    include Redis::Objects
    include Plines::RedisObjectsHelpers

    set :pending_job_jids
    set :completed_job_jids
    hash_key :meta

    def initialize(pipeline, id)
      super(pipeline, id)
      meta["created_at"] ||= Time.now.iso8601
      yield self if block_given?
    end

    def add_job(jid, *external_dependencies)
      pending_job_jids << jid
      EnqueuedJob.create(jid, *external_dependencies)
      external_dependencies.each do |dep|
        external_dependency_sets[dep] << jid
      end
    end

    def job_jids
      pending_job_jids | completed_job_jids
    end

    def mark_job_as_complete(jid)
      moved, pending_count, complete_count = redis.multi do
        pending_job_jids.move(jid, completed_job_jids)
        pending_job_jids.length
        completed_job_jids.length
      end

      unless moved == 1
        raise ArgumentError,
          "Jid #{jid} cannot be marked as complete for " +
          "job batch #{id} since it is not pending"
      end

      if _complete?(pending_count, complete_count)
        meta["completed_at"] = Time.now.iso8601
        set_expiration!
      end
    end

    def complete?
      _complete?(pending_job_jids.length, completed_job_jids.length)
    end

    def resolve_external_dependency(dep_name)
      external_dependency_sets[dep_name].each do |jid|
        EnqueuedJob.new(jid).resolve_external_dependency(dep_name) do
          job = pipeline.qless.job(jid)
          job.move(pipeline.default_queue.name) if job
        end
      end
    end

    def created_at
      time_from "created_at"
    end

    def completed_at
      time_from "completed_at"
    end

    def cancelled?
      meta["cancelled"] == "1"
    end

    def cancel!
      pending_job_jids.each { |jid| cancel_job(jid) }
      meta["cancelled"] = "1"
      set_expiration!
    end

  private

    def _complete?(pending_size, complete_size)
      pending_size == 0 && complete_size > 0
    end

    def time_from(meta_entry)
      date_string = meta[meta_entry]
      Time.iso8601(date_string) if date_string
    end

    def set_expiration!
      keys_to_expire = declared_redis_object_keys.to_set

      each_enqueued_job do |job|
        keys_to_expire.merge(job.declared_redis_object_keys)

        job.all_external_dependencies.each do |dep|
          keys_to_expire << external_dependency_sets[dep].key
        end
      end

      pipeline.set_expiration_on(*keys_to_expire)
    end

    def each_enqueued_job
      job_jids.each do |jid|
        yield EnqueuedJob.new(jid)
      end
    end

    def cancel_job(jid)
      # Cancelled jobs can no longer be fetched.
      return unless job = pipeline.qless.job(jid)

      # Qless doesn't let you cancel a job that has dependents,
      # so we must cancel them first, which will "undepend" the
      # job automatically.
      job.dependents.each { |dep_jid| cancel_job(dep_jid) }

      job.cancel
    end

    def external_dependency_sets
      @external_dependency_sets ||= Hash.new do |hash, dep|
        key = [self.class.redis_prefix, id, "ext_deps", dep].join(':')
        hash[dep] = Redis::Set.new(key, self.class.redis)
      end
    end
  end
end

