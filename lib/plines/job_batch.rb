require 'plines/enqueued_job'
require 'redis/objects'

module Plines
  # Represents a group of jobs that are enqueued together as a batch,
  # based on the step dependency graph.
  class JobBatch < Struct.new(:id)
    include Redis::Objects

    set :pending_job_jids
    set :completed_job_jids
    hash_key :meta

    def self.create(batch_key, jids)
      new(batch_key).tap do |batch|
        jids.each do |jid|
          batch.pending_job_jids << jid
        end
      end
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
      unless pending_job_jids.move(jid, completed_job_jids)
        raise ArgumentError,
          "Jid #{jid} cannot be marked as complete for " +
          "job batch #{id} since it is not pending"
      end
    end

    def complete?
      pending_job_jids.empty? && !completed_job_jids.empty?
    end

    def resolve_external_dependency(dep_name)
      external_dependency_sets[dep_name].each do |jid|
        EnqueuedJob.new(jid).resolve_external_dependency(dep_name)
      end
    end

    def cancelled?
      meta["cancelled"] == "1"
    end

    def cancel!
      pending_job_jids.each { |jid| cancel_job(jid) }
      meta["cancelled"] = "1"
    end

  private

    def cancel_job(jid)
      # Cancelled jobs can no longer be fetched.
      return unless job = Plines.qless.job(jid)

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

