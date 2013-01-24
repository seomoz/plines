module Plines
  # Stores global Plines configuration.
  class Configuration
    # Raised when there is a configuration error.
    class Error < StandardError; end

    TWO_MONTHS_IN_SECONDS = 2 * 30 * 24 * 60 * 60

    def initialize
      batch_list_key { raise Error, "batch_list_key has not been configured" }
      qless_job_options { |job| {} }
      self.data_ttl_in_seconds = TWO_MONTHS_IN_SECONDS
      @callbacks = Hash.new { |h, k| h[k] = [] }
    end

    attr_writer :qless
    def qless
      @qless ||= if @redis
        Qless::Client.new(redis: @redis)
      else
        Qless::Client.new
      end
    end

    attr_writer :redis
    def redis
      @redis ||= if @qless
        @qless.redis
      else
        Redis.new
      end
    end

    def batch_list_key(&block)
      @batch_list_key_block = block
    end

    def batch_list_key_for(batch_data)
      @batch_list_key_block[batch_data]
    end

    attr_accessor :data_ttl_in_seconds
    def data_ttl_in_milliseconds
      (data_ttl_in_seconds * 1000).to_i
    end

    attr_reader :qless_job_options_block
    def qless_job_options(&block)
      @qless_job_options_block = block
    end

    def after_job_batch_cancellation(&block)
      @callbacks[:after_job_batch_cancellation] << block
    end

    def notify(callback_type, *args)
      @callbacks[callback_type].each do |callback|
        callback.call(*args)
      end
    end
  end
end

