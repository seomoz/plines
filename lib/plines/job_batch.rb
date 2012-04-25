module Plines
  # Represents a group of jobs that are enqueued together as a batch,
  # based on the step dependency graph.
  class JobBatch
    attr_reader :batch_key

    def initialize(batch_key, redis=Plines.redis, &block)
      @batch_key = batch_key
      @redis_root_key = "plines:job_batches:#{batch_key}"
      @redis = redis
      instance_eval(&block) if block_given?
    end

    def self.create(batch_key, jids, redis=Plines.redis)
      new(batch_key, redis) do
        jids.each do |jid|
          redis.sadd redis_key_for("pending"), jid
        end
      end
    end

    def job_jids
      @redis.smembers redis_key_for("pending")
    end

  private

    def redis_key_for(state)
      [@redis_root_key, state].join(':')
    end
  end
end

