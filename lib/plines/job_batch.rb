require 'time'
require 'json'
require 'plines/redis_objects'
require 'plines/indifferent_hash'

module Plines
  # Represents a group of jobs that are enqueued together as a batch,
  # based on the step dependency graph.
  class JobBatch < Struct.new(:pipeline, :id)
    include Plines::RedisObjectsHelpers

    JobNotPendingError = Class.new(ArgumentError)

    set :pending_job_jids
    set :completed_job_jids
    set :timed_out_external_deps

    hash_key :meta
    attr_reader :qless, :redis

    def initialize(qless, pipeline, id)
      @qless = qless
      @redis = qless.redis
      @allowed_to_add_external_deps = false
      super(pipeline, id)
      yield self if block_given?
    end

    BATCH_DATA_KEY = "batch_data"
    EXT_DEP_KEYS_KEY = "ext_dep_keys"

    # We use find/create in place of new for both
    # so that the semantics of the two cases are clear.
    private_class_method :new

    CannotFindExistingJobBatchError = Class.new(StandardError)

    def self.find(qless, pipeline, id)
      new(qless, pipeline, id) do |inst|
        unless inst.created_at
          raise CannotFindExistingJobBatchError,
            "Cannot find an existing job batch for #{pipeline} / #{id}"
        end

        yield inst if block_given?
      end
    end

    JobBatchAlreadyCreatedError = Class.new(StandardError)
    AddingExternalDependencyNotAllowedError = Class.new(StandardError)

    def self.create(qless, pipeline, id, batch_data)
      new(qless, pipeline, id) do |inst|
        if inst.created_at
          raise JobBatchAlreadyCreatedError,
            "Job batch #{pipeline} / #{id} already exists"
        end

        inst.meta["created_at"]   = Time.now.iso8601
        inst.meta[BATCH_DATA_KEY] = JSON.dump(batch_data)

        inst.populate_external_deps_meta { yield inst if block_given? }
      end
    end

    def populate_external_deps_meta
      @allowed_to_add_external_deps = true
      yield
      ext_deps = external_deps | newly_added_external_deps.to_a
      meta[EXT_DEP_KEYS_KEY] = JSON.dump(ext_deps)
    ensure
      @allowed_to_add_external_deps = false
    end

    def newly_added_external_deps
      @newly_added_external_deps ||= []
    end

    def external_deps
      if keys = meta[EXT_DEP_KEYS_KEY]
        decode(keys)
      else
        []
      end
    end

    def add_job(jid, *external_dependencies)
      pending_job_jids << jid

      unless @allowed_to_add_external_deps || external_dependencies.none?
        raise AddingExternalDependencyNotAllowedError, "You cannot add jobs " +
          "with external dependencies after creating the job batch."
      else
        external_dependencies.each do |dep|
          newly_added_external_deps << dep
          external_dependency_sets[dep] << jid
        end

        EnqueuedJob.create(qless, pipeline, jid, *external_dependencies)
      end
    end

    def job_jids
      pending_job_jids | completed_job_jids
    end

    def jobs
      job_jids.map { |jid| EnqueuedJob.new(qless, pipeline, jid) }
    end

    def job_repository
      qless.jobs
    end

    def pending_qless_jobs
      pending_job_jids.map do |jid|
        job_repository[jid]
      end.compact
    end

    def qless_jobs
      job_jids.map do |jid|
        job_repository[jid]
      end.compact
    end

    def mark_job_as_complete(jid)
      moved, pending_count, complete_count = redis.multi do
        pending_job_jids.move(jid, completed_job_jids)
        pending_job_jids.length
        completed_job_jids.length
      end

      unless moved
        raise JobNotPendingError,
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
      jids = external_dependency_sets[dep_name]

      update_external_dependency \
        dep_name, :resolve_external_dependency, jids

      cancel_timeout_job_jid_set_for(dep_name)
    end

    def timeout_external_dependency(dep_name, jids)
      update_external_dependency \
        dep_name, :timeout_external_dependency, Array(jids)

      timed_out_external_deps << dep_name
    end

    def has_unresolved_external_dependency?(dep_name)
      external_dependency_sets[dep_name].any? do |jid|
        EnqueuedJob.new(qless, pipeline, jid)
                   .unresolved_external_dependencies.include?(dep_name)
      end
    end

    def timed_out_external_dependencies
      timed_out_external_deps.to_a
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

    CannotCancelError = Class.new(StandardError)

    def cancel!
      if complete?
        raise CannotCancelError,
          "JobBatch #{id} is already complete and cannot be cancelled"
      end

      perform_cancellation
    end

    def cancel
      return false if complete?
      perform_cancellation
    end

    def data
      data = decode(meta[BATCH_DATA_KEY])
      data && IndifferentHash.from(data)
    end

    def track_timeout_job(dep_name, jid)
      timeout_job_jid_sets[dep_name] << jid
    end

    def timeout_job_jid_sets
      @timeout_job_jid_sets ||= Hash.new do |hash, dep|
        key = [key_prefix, "timeout_job_jids", dep].join(':')
        hash[dep] = Redis::Set.new(key, redis)
      end
    end

    def spawn_copy(overrides = {})
      overrides = JSON.parse(JSON.dump overrides)
      pipeline.enqueue_jobs_for(data.merge overrides)
    end

  private

    def perform_cancellation
      qless.bulk_cancel(job_jids)

      external_deps.each do |key|
        cancel_timeout_job_jid_set_for(key)
      end

      meta["cancelled"] = "1"
      set_expiration!
      pipeline.configuration.notify(:after_job_batch_cancellation, self)
    end

    def update_external_dependency(dep_name, meth, jids)
      jids.each do |jid|
        EnqueuedJob.new(qless, pipeline, jid).send(meth, dep_name)
      end
    end

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
          keys_to_expire << timeout_job_jid_sets[dep].key
        end
      end

      set_expiration_on(*keys_to_expire)
    end

    def each_enqueued_job
      job_jids.each do |jid|
        yield EnqueuedJob.new(qless, pipeline, jid)
      end
    end

    def external_dependency_sets
      @external_dependency_sets ||= Hash.new do |hash, dep|
        key = [key_prefix, "ext_deps", dep].join(':')
        hash[dep] = Redis::Set.new(key, redis)
      end
    end

    def decode(string)
      string && JSON.load(string)
    end

    def cancel_timeout_job_jid_set_for(dep_name)
      timeout_job_jid_set = timeout_job_jid_sets[dep_name]
      timeout_job_jid_set.each { |jid| gracefully_cancel(jid) }
      timeout_job_jid_set.del
    end

    def gracefully_cancel(jid)
      job = job_repository[jid]
      job && job.cancel
    end

    def set_expiration_on(*redis_keys)
      redis_keys.each do |key|
        redis.pexpire(key, pipeline.configuration.data_ttl_in_milliseconds)
      end
    end
  end
end

