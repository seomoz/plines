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
    # user_data is a redis hash that can be updated by applications
    # (external to plines).
    hash_key :user_data
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
    CREATE_OPTIONS_KEY = "create_options"

    # We use find/create in place of new for both
    # so that the semantics of the two cases are clear.
    private_class_method :new

    CannotFindExistingJobBatchError = Class.new(StandardError)

    def self.find(qless, pipeline, id)
      new(qless, pipeline, id) do |inst|
        unless inst.creation_started_at
          raise CannotFindExistingJobBatchError,
            "Cannot find an existing job batch for #{pipeline} / #{id}"
        end

        yield inst if block_given?
      end
    end

    JobBatchAlreadyCreatedError = Class.new(StandardError)
    AddingExternalDependencyNotAllowedError = Class.new(StandardError)

    def self.create(qless, pipeline, id, batch_data, options = {}, &block)
      new(qless, pipeline, id) do |inst|
        inst.send(:initialize_new_batch, batch_data, options, &block)
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
      !!completed_at
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

    module InconsistentStateError
      def self.===(exn)
        Qless::LuaScriptError === exn &&
          exn.message.include?('InconsistentTimeoutState')
      end
    end

    def awaiting_external_dependency?(dep_name)
      lua.job_batch_awaiting_external_dependency?(
        job_batch: self,
        dependency_name: dep_name
      )
    rescue InconsistentStateError
      raise NotImplementedError, "External dependency #{dep_name} is in a " +
        "hybrid state in which it has timed out for some but not all jobs. " +
        "We don't support this state yet and may change the plines data " +
        "model so this state is no longer possible in the future."
    end

    def creation_started_at
      time_from("creation_started_at") || time_from("created_at")
    end
    # Alias for backwards compatibility
    alias created_at creation_started_at

    def creation_completed_at
      time_from "creation_completed_at"
    end

    def completed_at
      time_from "completed_at"
    end

    def cancelled_at
      time_from "cancelled_at"
    end

    def cancelled?
      !!cancelled_at || (meta["cancelled"] == "1")
    end

    def creation_in_progress?
      meta_values = meta.bulk_get(:creation_completed_at,
                                  :created_at,
                                  :creation_in_progress)

      if meta_values[:created_at]
        # This job batch was created before we updated how we tracked creation
        # in progress. The old way is through the creation_in_progress flag.
        !!meta_values[:creation_in_progress]
      else
        # The new way uses creation_started_at/creation_completed_at; if the
        # latter is set then creation is complete.
        !meta_values[:creation_completed_at]
      end
    end

    def cancellation_reason
      meta["cancellation_reason"]
    end

    def creation_reason
      meta["creation_reason"]
    end

    def timeout_reduction
      @timeout_reduction ||= meta["timeout_reduction"].to_i
    end

    def spawned_from
      return @spawned_from if defined?(@spawned_from)

      if id = spawned_from_id
        @spawned_from = self.class.find(qless, pipeline, id)
      else
        @spawned_from = nil
      end
    end

    def spawned_from_id
      meta['spawned_from_id']
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

    def cancel!(options = {})
      if complete?
        raise CannotCancelError,
          "JobBatch #{id} is already complete and cannot be cancelled"
      end

      perform_cancellation(options)
    end

    def cancel(options = {})
      return false if complete?
      perform_cancellation(options)
      true
    end

    def data
      data = decode(meta[BATCH_DATA_KEY])
      data && IndifferentHash.from(data)
    end

    def create_options
      options = decode(meta[CREATE_OPTIONS_KEY])
      options && IndifferentHash.from(options)
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

    SpawnOptions = Struct.new(:data_overrides, :timeout_reduction, :reason)

    def spawn_copy
      options = SpawnOptions.new({})
      yield options if block_given?
      overrides = JSON.parse(JSON.dump options.data_overrides)

      pipeline.enqueue_jobs_for(data.merge(overrides), {
        spawned_from_id: id,
        timeout_reduction: options.timeout_reduction || 0,
        reason: options.reason
      })
    end

    def user_data_keys
      user_data.keys
    end

    def user_data_get keys
      if keys.size > 0
        user_data.bulk_get *keys
      else
        user_data.all
      end
    end

    def user_data_set hash
      user_data.bulk_set hash
    end

  private

    # we manage these keys and don't want them in `create_options`
    SPECIAL_OPTION_KEYS = [:timeout_reduction, :spawned_from_id, :reason]

    def populate_meta_for_create(batch_data, options)
      opts_to_store = options.reject { |k, v| SPECIAL_OPTION_KEYS.include?(k) }

      metadata = {
        creation_started_at: Time.now.getutc.iso8601,
        timeout_reduction: options.fetch(:timeout_reduction, 0),
        BATCH_DATA_KEY => JSON.dump(batch_data),
        CREATE_OPTIONS_KEY => JSON.dump(opts_to_store),
      }

      if (reason = options[:reason])
        metadata[:creation_reason] = reason
      end

      if (spawned_from_id = options[:spawned_from_id])
        metadata[:spawned_from_id] = spawned_from_id
      end

      meta.bulk_set(metadata)
      @timeout_reduction = metadata.fetch(:timeout_reduction)
    end

    SomeJobsFailedToCancelError = Class.new(StandardError)
    CreationInStillInProgressError = Class.new(StandardError)

    def perform_cancellation(options)
      return true if cancelled?

      if creation_in_progress? && !creation_appears_to_be_stuck?
        raise CreationInStillInProgressError,
          "#{id} is still being created (started " +
          "#{Time.now - creation_started_at} seconds ago)"
      end

      qless.bulk_cancel(job_jids)
      verify_all_jobs_cancelled

      external_deps.each do |key|
        cancel_timeout_job_jid_set_for(key)
      end

      meta["cancellation_reason"] = options[:reason] if options.key?(:reason)
      meta["cancelled_at"] = Time.now.getutc.iso8601
      set_expiration!
      pipeline.configuration.notify(:after_job_batch_cancellation, self)
    end

    STUCK_BATCH_CREATION_TIMEOUT = 60 * 60 # 1 hour
    def creation_appears_to_be_stuck?
      age_in_seconds = Time.now - creation_started_at
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
      string && JSON.parse(string)
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
      @lua ||= Plines::Lua.new(qless)
    end

    def with_batch_creation_exception_logging
      yield
    rescue Exception => e
      pipeline.configuration.logger.error(
        "Aborting creation of plines JobBatch #{pipeline.name} #{id}: " \
        "#{e.class.name}: #{e.message} (#{e.backtrace.first})"
      )

      raise
    end

    def initialize_new_batch(batch_data, options)
      if creation_started_at
        raise JobBatchAlreadyCreatedError,
          "Job batch #{pipeline} / #{id} already exists"
      end

      with_batch_creation_exception_logging do
        populate_meta_for_create(batch_data, options)

        populate_external_deps_meta { yield self if block_given? }
        meta[:creation_completed_at] = Time.now.getutc.iso8601
      end
    end
  end
end

