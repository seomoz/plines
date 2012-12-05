require 'redis/objects'

module Plines
  # Represents a list of job batches that are grouped by
  # some common trait (such as a user id).
  class JobBatchList < Struct.new(:pipeline, :key)
    include Redis::Objects

    counter :last_batch_num

    def most_recent_batch
      batch_num = last_batch_num.value
      return nil if batch_num.zero?
      JobBatch.find(pipeline, batch_id_for(batch_num))
    end

    def create_new_batch(batch_data)
      JobBatch.create(pipeline,
                      batch_id_for(last_batch_num.increment),
                      batch_data)
    end

    def each(&block)
      return enum_for(:each) unless block_given?

      1.upto(last_batch_num.value) do |num|
        yield JobBatch.find(pipeline, batch_id_for(num))
      end
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

