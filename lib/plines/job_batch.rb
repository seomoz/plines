require 'redis/objects'

module Plines
  # Represents a group of jobs that are enqueued together as a batch,
  # based on the step dependency graph.
  class JobBatch < Struct.new(:id)
    include Redis::Objects

    set :pending_job_jids
    set :completed_job_jids

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

=begin
    def cancelled?
      false
    end

    def cancel!
      pending_job_jids.each do |jid|
        job = Plines.qless.job(jid)
        job.cancel
      end
    end
=end
  end
end

