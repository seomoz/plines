module Plines
  # Provides a way to atomically start multiple job batches.
  class AtomicJobBatchStarter
    def initialize(pipeline)
      @pipeline = pipeline
      @created_batches = []
    end

    def enqueue_jobs_for(batch_data, options = {}, &block)
      @pipeline.enqueue_jobs_for(
        batch_data, options.merge(leave_paused: true), &block
      ).tap do |jb|
        @created_batches << jb
      end
    end

    def atomically_start_created_batches
      return if @created_batches.empty?

      redises = @created_batches.map(&:redis).uniq
      if redises.one?
        redises.first.multi do
          @created_batches.each(&:unpause)
        end
      else
        raise MoreThanOneRedisServerError,
          "Cannot atomically unpause batches from " \
          "#{redises.count} redis servers"
      end
    end

    MoreThanOneRedisServerError = Class.new(StandardError)
  end
end
