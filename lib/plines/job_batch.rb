require 'time'
require 'json'
require 'plines/redis_objects'
require 'plines/indifferent_hash'
require 'plines/lua'

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

    def self.create(qless, pipeline, id, batch_data, options = {})
      new(qless, pipeline, id) do |inst|
        if inst.created_at
          raise JobBatchAlreadyCreatedError,
            "Job batch #{pipeline} / #{id} already exists"
        end

        inst.send(:populate_meta_for_create, batch_data, options)

        inst.populate_external_deps_meta { yield inst if block_given? }
        inst.meta.delete(:creation_in_progress)
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

    def complete_job(qless_job)
      qless_job.note_state_change(:complete) do
        lua.complete_job(self, qless_job)
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

    def creation_in_progress?
      meta["creation_in_progress"] == "1"
    end

    def timeout_reduction
      @timeout_reduction ||= meta["timeout_reduction"].to_i
    end

    def spawned_from
      return @spawned_from if defined?(@spawned_from)

      if id = meta["spawned_from_id"]
        @spawned_from = self.class.find(qless, pipeline, id)
      else
        @spawned_from = nil
      end
    end

    def in_terminal_state?
      cancelled? || complete?
    end

    CannotDeleteError = Class.new(StandardError)

    def delete
      unless in_terminal_state?
        raise CannotDeleteError,
          "JobBatch #{id} is not in a terminal state and cannot be deleted"
      end

      lua.delete!(self)
    end

    def delete!
      cancel
      lua.delete!(self)
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

    SpawnOptions = Struct.new(:data_overrides, :timeout_reduction)

    def spawn_copy
      options = SpawnOptions.new({})
      yield options if block_given?
      overrides = JSON.parse(JSON.dump options.data_overrides)

      pipeline.enqueue_jobs_for(data.merge(overrides), {
        spawned_from_id: id,
        timeout_reduction: options.timeout_reduction || 0
      })
    end

  private

    def populate_meta_for_create(batch_data, options)
      metadata = {
        created_at: Time.now.getutc.iso8601,
        timeout_reduction: 0,
        BATCH_DATA_KEY => JSON.dump(batch_data),
        creation_in_progress: 1
      }.merge(options)

      meta.bulk_set(metadata)
      @timeout_reduction = metadata.fetch(:timeout_reduction)
    end

    SomeJobsFailedToCancelError = Class.new(StandardError)
    CreationInStillInProgressError = Class.new(StandardError)

    def perform_cancellation
      return true if cancelled?

      if creation_in_progress? && !creation_appears_to_be_stuck?
        raise CreationInStillInProgressError,
          "#{id} is still being created (started " +
          "#{Time.now - created_at} seconds ago)"
      end

      qless.bulk_cancel(job_jids)
      verify_all_jobs_cancelled

      external_deps.each do |key|
        cancel_timeout_job_jid_set_for(key)
      end

      meta["cancelled"] = "1"
      set_expiration!
      pipeline.configuration.notify(:after_job_batch_cancellation, self)
    end

    STUCK_BATCH_CREATION_TIMEOUT = 6 * 60 * 60 # six hours
    def creation_appears_to_be_stuck?
      age_in_seconds = Time.now - created_at
      age_in_seconds >= STUCK_BATCH_CREATION_TIMEOUT
    end

    def verify_all_jobs_cancelled
      jobs = qless_jobs.reject { |j| j.state == "complete" }
      return if jobs.none?

      raise SomeJobsFailedToCancelError,
        "#{jobs.size} jobs failed to cancel: #{jobs.inspect}"
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
      lua.expire_job_batch(self)
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

    def lua
      @lua ||= Plines::Lua.new(qless.redis)
    end
  end
end

