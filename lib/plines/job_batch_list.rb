require 'plines/redis_objects'

module Plines
  # Represents a list of job batches that are grouped by
  # some common trait (such as a user id).
  class JobBatchList
    include Enumerable
    include Plines::RedisObjectsHelpers

    counter :last_batch_num
    attr_reader :qless, :redis, :pipeline, :key

    def initialize(pipeline, key)
      @pipeline = pipeline
      @key      = key
      @qless    = pipeline.configuration.qless_client_for(key)
      @redis    = @qless.redis
    end

    def most_recent_batch
      batch_num = last_batch_num.value
      return nil if batch_num.zero?
      JobBatch.find(qless, pipeline, batch_id_for(batch_num))
    end

    def create_new_batch(batch_data, options = {}, &blk)
      JobBatch.create(qless, pipeline,
                      batch_id_for(last_batch_num.increment),
                      batch_data, options, &blk)
    end

    def each
      return enum_for(__method__) unless block_given?

      each_id do |id|
        begin
          yield JobBatch.find(qless, pipeline, id)
        rescue JobBatch::CannotFindExistingJobBatchError
          # We can't yield a batch we can't find!
        end
      end
    end

    def each_id
      return enum_for(__method__) unless block_given?

      1.upto(last_batch_num.value) do |num|
        yield batch_id_for(num)
      end
    end

    def all_with_external_dependency_timeout(dep_name)
      select do |batch|
        batch.timed_out_external_deps.include?(dep_name)
      end
    end

    def all_with_external_dependency_timeouts
      reject do |batch|
        batch.timed_out_external_deps.empty?
      end
    end

    def ==(other)
      other.is_a?(JobBatchList) &&
      pipeline == other.pipeline &&
      key == other.key
    end
    alias eql? ==

    def hash
      [pipeline, key].hash
    end

  private

    alias id key

    def batch_id_for(number)
      [id, number].join(':')
    end
  end
end

