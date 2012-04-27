require 'redis/objects'
require 'plines/job_batch'

module Plines
  # Represents a list of job batches that are grouped by
  # some common trait (such as a user id).
  class JobBatchList < Struct.new(:id)
    include Redis::Objects

    counter :last_batch_num

    def most_recent_batch
      batch_num = last_batch_num.value
      return nil if batch_num.zero?
      JobBatch.new(batch_id_for(batch_num))
    end

    def create_new_batch(jids)
      JobBatch.create(batch_id_for(last_batch_num.increment), jids)
    end

    def self.for(batch_data)
      id = Plines.configuration.batch_list_key_for(batch_data)
      JobBatchList.new(id)
    end

  private

    def batch_id_for(number)
      [id, number].join(':')
    end
  end
end

