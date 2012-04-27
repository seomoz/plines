require 'redis/objects'

module Plines
  # Represents a group of jobs that are enqueued together as a batch,
  # based on the step dependency graph.
  class JobBatch < Struct.new(:id)
    include Redis::Objects

    set :pending_job_jids

    def self.create(batch_key, jids)
      new(batch_key).tap do |batch|
        jids.each do |jid|
          batch.pending_job_jids << jid
        end
      end
    end

    def job_jids
      pending_job_jids
    end
  end
end

