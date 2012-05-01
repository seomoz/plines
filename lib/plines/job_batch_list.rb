require 'redis/objects'
require 'plines/job_batch'

module Plines
  # Represents a list of job batches that are grouped by
  # some common trait (such as a user id).
  class JobBatchList < Struct.new(:pipeline, :key)
    include Redis::Objects

    counter :last_batch_num

    def most_recent_batch
      batch_num = last_batch_num.value
      return nil if batch_num.zero?
      JobBatch.new(pipeline, batch_id_for(batch_num))
    end

    def create_new_batch
      JobBatch.new(pipeline, batch_id_for(last_batch_num.increment))
    end

  private

    # needed by the redis objects counter
    def id
      @id ||= [pipeline.name, key].join(':')
    end

    def batch_id_for(number)
      [id, number].join(':')
    end
  end
end

