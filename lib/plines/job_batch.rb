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
  end
end

