require 'time'
require 'json'
require 'plines/redis_objects'

module Plines
  # Represents a group of jobs that are enqueued together as a batch,
  # based on the step dependency graph.
  class JobBatch < Struct.new(:pipeline, :id)
    include Plines::RedisObjectsHelpers

    JobNotPendingError = Class.new(ArgumentError)

    set :pending_job_jids
    set :completed_job_jids
    hash_key :meta
    attr_reader :qless, :redis

    def initialize(qless, pipeline, id)
      @qless = qless
      @redis = qless.redis
      super(pipeline, id)
      yield self if block_given?
    end

    BATCH_DATA_KEY = "batch_data"

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

    def self.create(qless, pipeline, id, batch_data)
      new(qless, pipeline, id) do |inst|
        if inst.created_at
          raise JobBatchAlreadyCreatedError,
            "Job batch #{pipeline} / #{id} already exists"
        end

        inst.meta["created_at"]   = Time.now.iso8601
        inst.meta[BATCH_DATA_KEY] = JSON.dump(batch_data)
        yield inst if block_given?
      end
    end

    def add_job(jid, *external_dependencies)
      pending_job_jids << jid
      external_dependencies.each do |dep|
        external_dependency_sets[dep] << jid
      end
      EnqueuedJob.create(qless, pipeline, jid, *external_dependencies)
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
      end
    end

    def qless_jobs
      job_jids.map do |jid|
        job_repository[jid]
      end
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

      timeout_job_jid_set = timeout_job_jid_sets[dep_name]
      timeout_job_jid_set.each { |jid| gracefully_cancel(jid) }
      timeout_job_jid_set.del
    end

    def timeout_external_dependency(dep_name, jids)
      update_external_dependency \
        dep_name, :timeout_external_dependency, Array(jids)
    end

    def has_unresolved_external_dependency?(dep_name)
      external_dependency_sets[dep_name].any? do |jid|
        EnqueuedJob.new(qless, pipeline, jid)
                   .unresolved_external_dependencies.include?(dep_name)
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
      job_jids.each { |jid| cancel_job(jid) }
      meta["cancelled"] = "1"
      set_expiration!
      pipeline.configuration.notify(:after_job_batch_cancellation, self)
    end

    def data
      decode(meta[BATCH_DATA_KEY])
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

  private

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

    def cancel_job(jid)
      # Cancelled jobs can no longer be fetched.
      return unless job = qless.jobs[jid]

      # Qless doesn't let you cancel a job that has dependents,
      # so we must cancel them first, which will "undepend" the
      # job automatically.
      job.dependents.each { |dep_jid| cancel_job(dep_jid) }

      job.cancel
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

